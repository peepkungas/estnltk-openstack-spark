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
    processes=2
fi

if [ $(ps aux | grep 'processtest.sh' | grep -v 'grep' | wc -l) -lt $processes ]; then
    answer=`/bin/sh /opt/estnltk-openstack-spark/spark_estnltk/spark_estnltk/scripts/processSequencefiles.sh | tee -a /opt/logs/process-sequencefiles.log`
    echo $answer
    if [ -z "$answer" -a "$answer" != " " ]; then
        echo "All files processed. Exiting" >> /opt/logs/process-sequencefiles.log
        exit
    fi
else
    echo "Already running. Exiting" >> /opt/logs/process-sequencefiles.log
fi