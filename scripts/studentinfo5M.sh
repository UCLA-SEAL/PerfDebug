#!/bin/bash
# https://stackoverflow.com/questions/59895/getting-the-source-directory-of-a-bash-script-from-within
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
$DIR/multiple_iterations.sh StudentInfo5MBaseline /tmp/studentinfo_5M_baseline.out 10
