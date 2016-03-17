#!/bin/bash
#if script work is not in progress
export HADOOP_HOME="/opt/cloudera/parcels/CDH-5.5.1-1.cdh5.5.1.p0.11/lib/hadoop/"
if [ $(ps aux | grep 'sequencefiles-to-hdfs-0.0.1-SNAPSHOT.jar' | grep -v 'grep' | wc -l) -eq 0 ];
then
        for n in "$@"
        do
        case $n in
                -p=*|--properties=*)
                properties="${n#*=}"
                ;;
                -i=*|--inputpath=*)
                inputpath="${n#*=}"
                ;;
                -o=*|--outputpath=*)
                outputpath="${n#*=}"
                ;;
        esac
        done

        file=/opt/estnltk-openstack-spark/sequencefiles_to_hdfs/target/sequencefiles-to-hdfs-0.0.1-SNAPSHOT.jar
        if [ ! -f "$file" ]; then 
                echo "Execution file '$file' not found."
                echo "Usage: <jar_file_location> <log_properties> <inputpath> <outputpath>"
                echo "Exiting..."
                exit
        fi

        if [ ! -f "$properties" ]; then 
                echo "Input path '$properties' not found."
                echo "Usage: <jar_file_location> <log_properties> <inputpath> <outputpath>"
                echo "Exiting"
                exit
        fi

        if [ ! -d "$inputpath" ]; then 
                echo "Input path '$inputpath' not found."
                echo "Usage: <jar_file_location> <log_properties> <inputpath> <outputpath>"
                echo "Exiting..."
                exit
        fi

        if [ ! -f "$file" ];
        then 
                echo "File $file does not exist."
                echo "Exiting..."
                exit
        fi

        if [ ! -f "$properties" ];
        then 
                echo "Properties file $properties does not exist."
                echo "Exiting..."
                exit
        fi

        answer=`java -Dlog4j.configuration=file:$properties -jar $file $inputpath $outputpath`
        if [ -z "$answer" -a "$answer" != " " ]; then
                echo "All files for inputpath '$inputpath' already successfully processed. Exiting."
                exit
        fi

        echo "Sequence files copying to HDFS has finished. Done..."

else
        echo "Sequence files copying to HDFS is already running. Exiting..."
fi
