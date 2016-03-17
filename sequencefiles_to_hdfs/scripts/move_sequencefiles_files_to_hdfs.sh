#!/bin/bash
#if script work is not in progress
if [ $(ps aux | grep 'sequencefiles-to-hdf' | grep -v 'grep' | wc -l) -eq 2 ];
then
        for n in "$@"
        do
        case $n in
                -f=*|--file=*)
                file="${n#*=}"
                ;;
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

        if [ ! -f "$file" ]; then 
                echo "Execution file '$file' not found."
                echo "Usage: <jar_file_location> <log_properties> <inputpath> <outputpath>"
                echo "Exiting"
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
                echo "Exiting"
                exit
        fi

        if [ ! -d "$outputpath" ]; then 
                echo "Output path '$outputpath' not found."
                echo "Usage: <jar_file_location> <log_properties> <inputpath> <outputpath>"
                echo "Exiting"
                exit
        fi

        if [ ! -f "$file" ];
        then 
                echo "File $file does not exist."
                echo "Exiting"
                exit
        fi

        if [ ! -f "$properties" ];
        then 
                echo "Properties file $properties does not exist."
                echo "Exiting"
                exit
        fi

        answer=`java -Dlog4j.configuration=file:$properties -jar $file $inputpath $outputpath`
        if [ -z "$answer" -a "$answer" != " " ]; then
                echo "All files for inputpath '$inputpath' already successfully processed. Exiting."
                exit
        fi
        echo "Copy of sequence files to HDFS has finished."
else
        echo "Copy of sequence files to HDFS is already running. Exiting"
fi
