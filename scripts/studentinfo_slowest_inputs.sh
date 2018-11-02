#!/bin/bash
set -e
export OUTFILE="/Users/jteoh/Code/perfdebug-separate-benchmarks/result-files/studentinfo_slowest_inputs_results.txt"
export CLASS="StudentInfo5M"
export DATA_FILE="/Users/jteoh/Code/Performance-Debug-Benchmarks/StudentInfo/studentData_5M.txt"
RECORD="ownz htpnuohgpx female 18 0 Biology" scripts/slowest_inputs_evaluation.sh
RECORD="nuvamqjhpm pqrzq female 18 0 Economics" scripts/slowest_inputs_evaluation.sh
RECORD="yqisuoat fuo female 23 2 Law" scripts/slowest_inputs_evaluation.sh
RECORD="fpqplbkax zrrlblkp female 20 1 IndustrialEngineering" scripts/slowest_inputs_evaluation.sh
RECORD="vlvpueqp wc female 19 0 IndustrialEngineering" scripts/slowest_inputs_evaluation.sh
RECORD="fqdnmsqt kjybgsphrntxbgk male 25 3 Law" scripts/slowest_inputs_evaluation.sh
RECORD="ppww zuotdxjphkyu female 19 0 Business" scripts/slowest_inputs_evaluation.sh
RECORD="wdokwpkzq zaajyyjqfhef female 20 1 Law" scripts/slowest_inputs_evaluation.sh
RECORD="wjorz h female 25 3 Economics" scripts/slowest_inputs_evaluation.sh
RECORD="boonhnwmbl bdliuhdczwb female 20 1 Law" scripts/slowest_inputs_evaluation.sh

cat $OUTFILE