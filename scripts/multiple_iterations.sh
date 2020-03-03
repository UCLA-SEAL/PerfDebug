#!/bin/bash

# CMD="java -cp /Users/jteoh/Code/perfdebug-baselines/target/scala-2.11/perfdebug-baselines-assembly-0.1.jar "
CLASS=${1}
OUTFILE=${2}
LOGFILE="/tmp/perfdebug-baseline-lastrun.log"
NUM_ITERATIONS=${3}
if [ -z "$CLASS" ]; then
  echo "Please specify class name"
  exit 1
elif [ -z "$OUTFILE" ]; then
  echo "Please specify an output file"
  exit 2
elif [ -z "$NUM_ITERATIONS" ]; then
  echo "Please specify number of iterations"
  exit 3
fi

# CMD+=$CLASS
CMD="sbt \"runMain $CLASS\""

rm -f $OUTFILE
rm -f $LOGFILE
echo "Printing output to $OUTFILE, log in $LOGFILE"
echo $CMD
for ((i = 1; i <= $NUM_ITERATIONS; i++)) ; do 
  echo "SCRIPT: iteration $i"
  eval $CMD 2>&1 | tee -a $LOGFILE | fgrep "Collect time: " | sed -En "s/[^0-9]*([0-9]{1,})[^0-9]*/\1/gp" >> $OUTFILE
  # $CMD 2>&1 | fgrep "Collect time: " | sed -En "s/[^0-9]*([0-9]{1,})[^0-9]*/\1/gp" >> $OUTFILE
done

paste -sd " " $OUTFILE