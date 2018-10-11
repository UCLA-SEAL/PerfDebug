#!/bin/bash
# https://gist.github.com/iangreenleaf/279849
trap "echo Exited!; exit;" SIGINT SIGTERM

# https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

NUM_ITERATIONS=${1:-10}
echo "NUM ITERATIONS: $NUM_ITERATIONS"
# full list of classes!
classes="StudentInfo WordCount InvertedIndex HistogramMovies HistogramRatings Weather TermVector"
# classes="HistogramMovies HistogramRatings"

OUTDIR="/tmp/perfdebug-separate-benchmarks"
mkdir -p $OUTDIR

OUTFILE=$OUTDIR/all_benchmark_results_perfdebug.txt

echo "FULL RESULTS COPIED IN $OUTFILE"
rm -f $OUTFILE
cd $DIR/.. #need to be in top level folder for sbt call.



#for file in $files; do
for class in $classes; do
	# https://stackoverflow.com/questions/2664740/extract-file-basename-without-path-and-extension-in-bash
	# basename=${file##*/}
	# class=${basename%.scala}
	echo "-----------" | tee -a $OUTFILE
	echo "CLASS: $class" | tee -a $OUTFILE
	$DIR/multiple_iterations.sh $class $NUM_ITERATIONS
	# only copy the results (note: these are conveniently printed in multiple_iterations.sh)
	paste -sd " " $OUTDIR/${class}-results >> $OUTFILE
	echo "-----------" | tee -a $OUTFILE
done;
echo "------------------RESULTS------------------"
# $DIR/multiple_iterations.sh
cat $OUTFILE