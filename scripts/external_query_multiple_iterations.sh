#!/bin/bash
# https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
trap "echo Iterations interrupted!; exit;" SIGINT SIGTERM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# example: scripts/external_query_multiple_iterations.sh local-1541101792462 1 FORWARD_SUM_AND_LINEAGE_INPUT_JOIN /Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo/studentData_5M.txt
# another: scripts/external_query_multiple_iterations.sh local-1541101792462 1 SLOWEST_INPUTS_QUERY /Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo/studentData_5M.txt
# logs would be in /tmp/perfdebug-separate-benchmarks/ExternalQueryDemo-local-1541101792462-...
CLASS="ExternalQueryDemo"
APP_ID=${1}
NUM_ITERATIONS=${2}
EXEC_MODE=${3}
DATA_FILES=${@:4} # assumed it's provided as needed.
if [ -z "$APP_ID" ]; then
  echo "Please specify app id"
  exit 1
elif [ -z "$NUM_ITERATIONS" ]; then
  echo "Please specify number of iterations"
  exit 2
elif [ -z "$EXEC_MODE" ]; then
  echo "Please specify performance query"
  exit 3
fi

# TODO continue edits here

CMD="sbt \"runMain $CLASS $APP_ID $EXEC_MODE $DATA_FILES\""
# echo $CMD
OUTDIR="/tmp/perfdebug-separate-benchmarks"
mkdir -p $OUTDIR 
FILETAG="${CLASS}-${APP_ID}-${EXEC_MODE}"
LOGFILE="$OUTDIR/$FILETAG-logs"
RESULTFILE="$OUTDIR/$FILETAG-results"
rm -f $RESULTFILE
rm -f $LOGFILE
echo "Printing collected times to $RESULTFILE, logs in $LOGFILE"
# echo $CMD

# we explicitly want to reuse the existing instance, since we need lineage data.
# $DIR/stop_ignite.sh

for ((i = 1; i <= $NUM_ITERATIONS; i++)) ; do 
  echo "SCRIPT: iteration $i ($CMD)" | tee -a $LOGFILE
  # not sure why eval is needed, but the "runMain $CLASS" portion errors out otherwise.
  eval $CMD 2>&1 | tee -a $LOGFILE | fgrep "Execution-only latency: " | sed -En "s/[^0-9]*([0-9]{1,})[^0-9]*/\1/gp" >> $RESULTFILE
done

echo "Results can be found in $RESULTFILE, logs in $LOGFILE"
paste -sd " " $RESULTFILE
