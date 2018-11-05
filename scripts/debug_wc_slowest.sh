#!/bin/bash
export OUTFILE="/tmp/weather_slowest_inputs_results_debug.txt"
export CLASS="Weather" 
export DATA_FILE="/Users/jteoh/Code/BigSummary-Experiments/experiments/WeatherAnalysis/data/part-00000"

RECORD="73952,28/6/2014,32 mm" scripts/slowest_inputs_evaluation_testing.sh