#!/bin/bash
set -e
export OUTFILE="/Users/jteoh/Code/perfdebug-separate-benchmarks/result-files/movieratings_slowest_inputs_results.txt"
export CLASS="HistogramRatings" 
export DATA_FILE="/Users/jteoh/Code/BigSummary-Experiments/experiments/MoviesAnalysis/data/file1s.data"
# TODO: just use an array dimwit.
RECORD="1060" scripts/slowest_inputs_evaluation.sh
RECORD="814" scripts/slowest_inputs_evaluation.sh
RECORD="1079" scripts/slowest_inputs_evaluation.sh
RECORD="413" scripts/slowest_inputs_evaluation.sh
RECORD="714" scripts/slowest_inputs_evaluation.sh
RECORD="260" scripts/slowest_inputs_evaluation.sh
RECORD="815" scripts/slowest_inputs_evaluation.sh
RECORD="1438" scripts/slowest_inputs_evaluation.sh
RECORD="744" scripts/slowest_inputs_evaluation.sh
RECORD="1770" scripts/slowest_inputs_evaluation.sh

cat $OUTFILE