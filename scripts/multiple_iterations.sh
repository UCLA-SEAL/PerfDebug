#!/bin/bash

CLASS=${1}
NUM_ITERATIONS=${2}
if [ -z "$CLASS" ]; then
  echo "Please specify class name"
  exit 1
elif [ -z "$NUM_ITERATIONS" ]; then
  echo "Please specify number of iterations"
  exit 3
fi

CMD="sbt \"runMain $CLASS\""
echo $CMD
LOGFILE="/tmp/$CLASS-logs"
RESULTFILE="/tmp/$CLASS-results"
rm -f $RESULTFILE
rm -f $LOGFILE
echo "Printing collected times to $RESULTFILE, logs in $LOGFILE"
echo $CMD
for ((i = 1; i <= $NUM_ITERATIONS; i++)) ; do 
  echo "SCRIPT: iteration $i" | tee -a $LOGFILE
  # not sure why eval is needed, but the "runMain $CLASS" portion errors out otherwise.
  eval $CMD 2>&1 | tee -a $LOGFILE | fgrep "Collect time: " | sed -En "s/[^0-9]*([0-9]{1,})[^0-9]*/\1/gp" >> $RESULTFILE
done

echo "Results can be found in $RESULTFILE, logs in $LOGFILE"
paste -sd " " $RESULTFILE
