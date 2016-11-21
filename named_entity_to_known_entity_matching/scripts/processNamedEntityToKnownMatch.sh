#!/bin/bash
filesToProcessPath="/processed/"
filesProcessed="/known_match_processed/"
processedFile="/opt/properties/known_match_processed/.processed"
timestamp=`date --iso-8601=seconds`
tmpManifestFile="/tmp/manifest_$timestamp.txt"
knownEntityDataCSV="spark-input-30122015.csv"
filesToProcess=`hadoop fs -ls $filesToProcessPath`

echo "$tmpManifestFile"

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
            #create manifest file
            echo $f >> "$tmpManifestFile"

            #Is YARN ResourceManager up
            timeout=`timeout 12 yarn node -list`
            answer=$?
            if [ ! $answer -eq 0 ]; then
                echo "YARN Service is down. Exiting..." >> /opt/logs/known_match_processed.log
                exit
            fi

            #Are any YARN nodemanagers up
            if [ $(yarn node -list | grep hadoop-ner | wc -l) -eq 0 ]; then
                echo "All YARN nodemanagers are down. Exiting..." >> /opt/logs/known_match_processed.log
                exit
            fi

            echo $f >> "$processedFile"
            echo "Processing file $filesProcessed$filename"
            `spark-submit --conf "spark.executor.memory=4g" file:///opt/estnltk-openstack-spark/named_entity_to_known_entity_matching/nerMatcher.py $tmpManifestFile $knownEntityDataCSV $filesProcessed$filename`
            `rm $tmpManifestFile`;
        fi
    fi

done