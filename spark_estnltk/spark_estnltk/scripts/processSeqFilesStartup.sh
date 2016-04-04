#!/bin/bash
if [ $(ps aux | grep 'processtest.sh' | grep -v 'grep' | wc -l) -lt 2 ];
then
  answer=`/bin/sh /opt/estnltk-openstack-spark/spark_estnltk/spark_estnltk/scripts/processtest.sh | tee -a /opt/logs/process-sequencefiles.log`
  echo $answer
  if [ -z "$answer" -a "$answer" != " " ]; then
    echo "All files processed. Exiting" >> /opt/logs/process-sequencefiles.log
    exit
  fi
else
  echo "Already running. Exiting" >> /opt/logs/process-sequencefiles.log
fi