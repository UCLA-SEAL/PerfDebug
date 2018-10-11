#!/bin/bash
# https://gist.github.com/iangreenleaf/279849
trap "echo Exited!; exit;" SIGINT SIGTERM

# https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
NUM_ITERATIONS=${1:-10}
echo "NUM ITERATIONS: $NUM_ITERATIONS"
# files=$(find ../src/ -name "*Baseline.scala")
classes="StudentInfoBaseline WordCountBaseline InvertedIndexBaseline HistogramMoviesBaseline HistogramRatingsBaseline WeatherBaseline TermVectorBaseline"
classes="HistogramMoviesBaseline HistogramRatingsBaseline"
OUTFILE=/tmp/all_benchmark_results_titian.txt

echo "FULL RESULTS COPIED IN $OUTFILE"
rm -f $OUTFILE
cd $DIR/.. #need to be in top level folder for sbt call.
#for file in $files; do
for class in $classes; do
	# https://stackoverflow.com/questions/2664740/extract-file-basename-without-path-and-extension-in-bash
	# basename=${file##*/}
	# class=${basename%.scala}
	$DIR/multiple_iterations.sh $class $NUM_ITERATIONS
	echo "-----------" >> $OUTFILE
	echo $class >> $OUTFILE
	paste -sd " " /tmp/${class}-results >> $OUTFILE
	echo "-----------" >> $OUTFILE
done;
echo "------------------RESULTS------------------"
# $DIR/multiple_iterations.sh
cat $OUTFILE