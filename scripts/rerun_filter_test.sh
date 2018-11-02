#!/bin/bash
TEMP_FILE="/tmp/FILTERED-${DATA_FILE##*/}" # basename of file
echo "Making copy of $DATA_FILE without record \"$RECORD\" in $TEMP_FILE"

grep -v "^${RECORD}" $DATA_FILE > $TEMP_FILE # MAIN COMMAND

scripts/stop_ignite.sh # MAIN COMMAND
scripts/start_ignite.sh # MAIN COMMAND

echo "RUNNING $CLASS WITH FILTERED RECORD DATA"
FILTERED_LOG="/tmp/$CLASS-filtered-log"

sbt "runMain $CLASS $TEMP_FILE" | tee $FILTERED_LOG # MAIN COMMAND
FILTERED_RUNTIME=$(sed -n "s/Collect time: \(.*\) ms/\1/p" $FILTERED_LOG)
echo "FILTERED RUNTIME: $FILTERED_RUNTIME"


echo "=======================" | tee -a /tmp/rerun_filter_results.txt
echo "$RECORD" | tee -a /tmp/rerun_filter_results.txt
echo "$FILTERED_RUNTIME" | tee -a /tmp/rerun_filter_results.txt
