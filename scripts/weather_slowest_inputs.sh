#!/bin/bash
# to get lines for delay, use something such as:
# sort "/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000" | uniq -u | gshuf -n 20 > /tmp/weather_uniq_shuf.out
set -e
export OUTFILE="/Users/jteoh/Code/perfdebug-separate-benchmarks/result-files/weather_slowest_inputs_results.txt"
export CLASS="Weather" 
export DATA_FILE="/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000"

RECORD="73952,28/6/2014,32 mm" scripts/slowest_inputs_evaluation.sh
RECORD="18090,17/4/1945,825 mm" scripts/slowest_inputs_evaluation.sh
RECORD="28415,3/7/2005,1589 mm" scripts/slowest_inputs_evaluation.sh
RECORD="45582,24/2/2005,655 mm" scripts/slowest_inputs_evaluation.sh
RECORD="59141,29/5/1917,1679 mm" scripts/slowest_inputs_evaluation.sh
RECORD="82870,13/8/2003,5.973933 ft" scripts/slowest_inputs_evaluation.sh
RECORD="45866,19/2/1903,3991 mm" scripts/slowest_inputs_evaluation.sh
RECORD="50615,18/2/1906,6.14904 ft" scripts/slowest_inputs_evaluation.sh
RECORD="50615,2/4/2011,3.779169 ft" scripts/slowest_inputs_evaluation.sh
RECORD="32817,5/9/1914,50 mm" scripts/slowest_inputs_evaluation.sh

cat $OUTFILE