#!/bin/bash
# example: 
# CLASS="StudentInfo5M" DATA_FILE="/Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo/studentData_5M.txt" RECORD="vlvpueqp wc female 19 0 IndustrialEngineering" DELAY="10000" scripts/slowest_inputs_evaluation.sh
if [ -z "${RECORD}" ]; then 
    echo "Please set a record via RECORD=\"(...)\" on the command line!"
    echo "if you need assistance picking records, consider gshuf"
    echo "\t eg: gshuf -n 10 studentData_5M.txt"
    exit 1
fi

if [ -z "${DATA_FILE}" ]; then
	echo "Please set the DATA_FILE environment variable first."
	exit 2
fi

if [ -z "${CLASS}" ]; then
	echo "Please set the CLASS environment variable first."
	exit 3
fi

DELAY=${DELAY:-10000} # 10 seconds by default
OUTFILE=${OUTFILE:-/tmp/slowest_inputs_results.txt}

#gshuf -n 10 studentData_5M.txt # or multiple, to get multiple results

echo "Restarting ignite..."
scripts/stop_ignite.sh # MAIN COMMAND
scripts/start_ignite.sh # MAIN COMMAND


TEMP_FILE="/tmp/FILTERED-${DATA_FILE##*/}" # basename of file
echo "Making copy of $DATA_FILE without record \"$RECORD\" in $TEMP_FILE"

grep -v "^${RECORD}" $DATA_FILE > $TEMP_FILE # MAIN COMMAND


ORIG_COUNT=$(wc -l $DATA_FILE | awk {'print $1'})
NEW_COUNT=$(wc -l $TEMP_FILE | awk {'print $1'})
if [ $((NEW_COUNT + 1)) -ne $ORIG_COUNT ]; then
	echo "ERROR: Randomly selected record was not unique"
	echo "Count before and after = $ORIG_COUNT vs $NEW_COUNT"
	exit 5
else
	echo "Removal of exactly one record confirmed."
fi

echo "RUNNING $CLASS WITH DELAY=$DELAY"
DELAY_LOG="/tmp/$CLASS-delayed-log"
sbt "runMain $CLASS $DATA_FILE \"$RECORD\" $DELAY" | tee $DELAY_LOG # MAIN COMMAND

echo "PARSING LOG FOR APP ID"
APP_ID=$(sed -n "s/SAVING APP \(.*\) LINEAGE DEPENDENCIES/\1/p" $DELAY_LOG)
DELAY_RUNTIME=$(sed -n "s/Collect time: \(.*\) ms/\1/p" $DELAY_LOG)
echo "APP ID: $APP_ID"
echo "DELAY_RUNTIME: $DELAY_RUNTIME"

QUERY_LOG="/tmp/$CLASS-slowestquery-log"
sbt "runMain ExternalQueryDemo $APP_ID BOTH_SLOWEST_INPUT_VERSIONS $DATA_FILE" | tee $QUERY_LOG # MAIN COMMAND

QUERY_RUNTIME=$(sed -n "s/Execution-only latency: \(.*\) ms/\1/p" $QUERY_LOG)
V1_RESULT=$(sed -n "s/V1 Record: \(.*\)/\1/p" $QUERY_LOG)
V1_IMPACT=$(sed -n "s/V1 Impact: \(.*\)/\1/p" $QUERY_LOG)
V2_RESULT=$(sed -n "s/V2 Record: \(.*\)/\1/p" $QUERY_LOG)
V2_IMPACT=$(sed -n "s/V2 Impact: \(.*\)/\1/p" $QUERY_LOG)
echo "APPROX QUERY TIME: $QUERY_RUNTIME"
echo "V1_RESULT: $V1_RESULT"
if [[ "$V1_RESULT" = "${RECORD}"* ]]; then
	echo -e "\tV1: POSITIVE MATCH"
else
	echo -e "\tV1: NEGATIVE MATCH"
fi

echo "V1_IMPACT: $V1_IMPACT"

if [[ "$V2_RESULT" = "$RECORD"* ]]; then
	echo -e "\tV2: POSITIVE MATCH"
else
	echo -e "\tV2: NEGATIVE MATCH"
fi

echo "V2_IMPACT: $V2_IMPACT"

scripts/stop_ignite.sh # MAIN COMMAND
scripts/start_ignite.sh # MAIN COMMAND

echo "RUNNING $CLASS WITH FILTERED RECORD DATA"
FILTERED_LOG="/tmp/$CLASS-filtered-log"

sbt "runMain $CLASS $TEMP_FILE" | tee $FILTERED_LOG # MAIN COMMAND
FILTERED_RUNTIME=$(sed -n "s/Collect time: \(.*\) ms/\1/p" $FILTERED_LOG)
echo "FILTERED RUNTIME: $FILTERED_RUNTIME"


echo "-----------------" | tee -a "$OUTFILE"
echo "Appending final results to $OUTFILE"
echo "FINAL RESULTS FOR $CLASS $DATA_FILE $RECORD -> $DELAY: " | tee -a $OUTFILE
echo "RECORD: $RECORD" | tee -a $OUTFILE
if [[ "$V1_RESULT" = "${RECORD}"* ]]; then
	echo -e "V1: POSITIVE MATCH" | tee -a $OUTFILE
else
	echo -e "V1: NEGATIVE MATCH" | tee -a $OUTFILE
fi
if [[ "$V2_RESULT" = "$RECORD"* ]]; then
	echo -e "V2: POSITIVE MATCH" | tee -a $OUTFILE
else
	echo -e "V2: NEGATIVE MATCH" | tee -a $OUTFILE
fi
echo "PerfTrace approx: $QUERY_RUNTIME"  | tee -a $OUTFILE
echo "V1_IMPACT: $V1_IMPACT" | tee -a $OUTFILE
echo "V2_IMPACT: $V2_IMPACT" | tee -a $OUTFILE
echo "Delayed  execution time: $DELAY_RUNTIME" | tee -a $OUTFILE
echo "Filtered execution time: $FILTERED_RUNTIME" | tee -a $OUTFILE
echo "PASTE-FRIENDLY:" | tee -a $OUTFILE
echo "$QUERY_RUNTIME" | tee -a $OUTFILE
echo "$V1_IMPACT" | tee -a $OUTFILE
echo "$V2_IMPACT" | tee -a $OUTFILE
echo "$DELAY_RUNTIME" | tee -a $OUTFILE
echo "$FILTERED_RUNTIME" | tee -a $OUTFILE

echo -e "$QUERY_RUNTIME\n$V1_IMPACT\n$V2_IMPACT\n$DELAY_RUNTIME\n$FILTERED_RUNTIME\n" | pbcopy
echo "Also copied to clipboard (if pbcopy is available)"
