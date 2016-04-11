#!/bin/bash
for n in "$@"
do
case $n in
    -p=*|--processes=*)
    processes="${n#*=}"
    ;;
esac
done

if [ ! -n "$processes" ]; then
    processes=1
fi

if [ $(ps aux | grep 'processEstlinkSpark.sh' | grep -v 'grep' | wc -l) -lt $processes ]; then
    answer=`/bin/sh /opt/estnltk-openstack-spark/spark_NER/scripts/processEstlinkSpark.sh | tee -a /opt/logs/process-estlink-spark.log`
    echo "Process ended. Exiting" >> /opt/logs/process-estlink-spark.log
else
    echo "Already running. Exiting" >> /opt/logs/process-estlink-spark.log
fi