#!/bin/bash

# /opt/spark-2.1/sbin/start-history-server.sh --properties-file history-conf/spark-history-minbar.conf

export SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=$PWD"
/opt/spark-2.1/sbin/start-history-server.sh
