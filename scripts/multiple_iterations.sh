#!/bin/bash
# https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
trap "echo Iterations interrupted!; exit;" SIGINT SIGTERM
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

CLASS=${1}
NUM_ITERATIONS=${2}
PROGRAM_ARGS=${@:3}
if [ -z "$CLASS" ]; then
  echo "Please specify class name"
  exit 1
elif [ -z "$NUM_ITERATIONS" ]; then
  echo "Please specify number of iterations"
  exit 3
fi

CMD="sbt \"runMain $CLASS $PROGRAM_ARGS\""

# echo $CMD
OUTDIR="/tmp/perfdebug-separate-benchmarks"
mkdir -p $OUTDIR 
LOGFILE="$OUTDIR/$CLASS-logs"
RESULTFILE="$OUTDIR/$CLASS-results"
rm -f $RESULTFILE
rm -f $LOGFILE
echo "Printing collected times to $RESULTFILE, logs in $LOGFILE"
# echo $CMD

$DIR/stop_ignite.sh # make sure no instances

for ((i = 1; i <= $NUM_ITERATIONS; i++)) ; do 
  echo "SCRIPT: iteration $i ($CMD)" | tee -a $LOGFILE
  $DIR/start_ignite.sh
  # THIS ONE IS DIFFERENT - need to start a fresh ignite instance and kill after, every time.
  # not sure why eval is needed, but the "runMain $CLASS" portion errors out otherwise.
  eval $CMD 2>&1 | tee -a $LOGFILE | fgrep "Collect time: " | sed -En "s/[^0-9]*([0-9]{1,})[^0-9]*/\1/gp" >> $RESULTFILE
  $DIR/stop_ignite.sh
done

echo "Results can be found in $RESULTFILE, logs in $LOGFILE"
paste -sd " " $RESULTFILE
