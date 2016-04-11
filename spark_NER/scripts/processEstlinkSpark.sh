#!/bin/bash
filesToProcessPath="/processed/"
filesProcessed="/estlink_spark_processed/"
processedFile="/opt/properties/estlink_spark_processed/.processed"
filesToProcess=`hadoop fs -ls $filesToProcessPath`

for f in $filesToProcess
do

    if [ "$f" != "${f%$filesToProcessPath*}" ]; then
        filename=${f##*/}
        exists=0

        while read p; do
        if [ "$f" = "$p" ]; then
            exists=1
            break
        fi
        done < "$processedFile"

        if [ "$exists" = 0 ]; then

            #Is YARN ResourceManager up
            timeout=`timeout 12 yarn node -list`
            answer=$?
            if [ ! $answer -eq 0 ]; then
                echo "YARN Service is down. Exiting..." >> /opt/logs/process-estlink-spark.log
                exit
            fi

            #Are any YARN nodemanagers up
            if [ $(yarn node -list | grep hadoop-ner | wc -l) -eq 0 ]; then
                echo "All YARN nodemanagers are down. Exiting..." >> /opt/logs/process-estlink-spark.log
                exit
            fi

            echo $f >> "$processedFile"
            echo "Processing file $filesProcessed$filename"
            `spark-submit --master yarn --deploy-mode cluster --conf spark.hadoop.validateOutputSpecs=false file:///opt/estnltk-openstack-spark/spark_NER/estlinkSpark.py $f /csv/spark-input-29122015.csv $filesProcessed$filename`
        fi
    fi
done