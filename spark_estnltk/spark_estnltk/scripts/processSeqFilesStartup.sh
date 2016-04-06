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

if [ $(ps aux | grep 'processSequencefiles.sh' | grep -v 'grep' | wc -l) -lt $processes ]; then
    answer=`/bin/sh /opt/estnltk-openstack-spark/spark_estnltk/spark_estnltk/scripts/processSequencefiles.sh | tee -a /opt/logs/process-sequencefiles.log`
    echo "Process ended. Exiting" >> /opt/logs/process-sequencefiles.log
else
    echo "Already running. Exiting" >> /opt/logs/process-sequencefiles.log
fi