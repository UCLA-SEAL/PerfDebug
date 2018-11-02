#!/bin/bash
# simple and easy.
TIMEOUT=${1:-10} # default time is the right-hand number
$IGNITE_HOME/bin/ignite.sh &
sleep $TIMEOUT # sleep a bit to ensure the client is up and running
