#!/bin/bash
set -e

export OUTFILE="/Users/jteoh/Code/perfdebug-separate-benchmarks/result-files/weather_slowest_inputs_results.txt"
export CLASS="Weather" 
export DATA_FILE="/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000"

DELAY_LOG="/tmp/$CLASS-delayed-log"
echo "Skipping execution of application and restarting of ignite - reusing previous cache"
# sbt "runMain $CLASS $DATA_FILE \"$RECORD\" $DELAY" | tee $DELAY_LOG # MAIN COMMAND

echo "PARSING LOG FOR APP ID"
APP_ID=$(sed -n "s/SAVING APP \(.*\) LINEAGE DEPENDENCIES/\1/p" $DELAY_LOG)
DELAY_RUNTIME=$(sed -n "s/Collect time: \(.*\) ms/\1/p" $DELAY_LOG)
echo "APP ID: $APP_ID"
echo "DELAY_RUNTIME: $DELAY_RUNTIME"

QUERY_LOG="/tmp/$CLASS-slowestquery-log"
sbt "runMain ExternalQueryDemo $APP_ID BOTH_SLOWEST_INPUT_VERSIONS $DATA_FILE 5" | tee $QUERY_LOG # MAIN COMMAND

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

echo "TEMPORARY STOP - JASON"
exit 1


# (((0,1220306) => 31881149, 0),10000)
# sed -e 's/\r$//' /Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000 > /tmp/weather-temp-file