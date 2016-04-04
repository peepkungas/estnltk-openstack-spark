#!/bin/bash
filesToProcessPath="/sequencefiles/"
filesProcessed="/processed/"
processedFile="/opt/properties/process_sequencefiles/.processed"
filesToProcess=`hadoop fs -ls $filesToProcessPath`

for f in $filesToProcess do
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
            echo $f >> "$processedFile"
            echo "Processing file $filesProcessed$filename"
            `spark-submit --master yarn --deploy-mode cluster --num-executors 4 --conf spark.hadoop.validateOutputSpecs=false /opt/estnltk-openstack-spark/spark_estnltk/spark_estnltk/process.py $f $filesProcessed$filename -lemma -ner`
        fi
    fi
done