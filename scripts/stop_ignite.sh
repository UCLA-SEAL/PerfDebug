#!/bin/bash
# taken from: https://github.com/apache/ignite/blob/master/modules/clients/src/test/bin/stop-nodes.sh
TIMEOUT=${1:-1}
for X in `$JAVA_HOME/bin/jps | grep -i -G .*CommandLineStartup | awk {'print $1'}`; do
    kill -9 $X
done

sleep $TIMEOUT # give it some time to stop.