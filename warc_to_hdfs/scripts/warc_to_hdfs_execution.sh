#!/bin/bash
#if script work is not in progress
if [ $(ps aux | grep 'import_to_hdfs' | grep -v 'grep' | wc -l) -eq 0 ];
then
        for n in "$@"
        do
        case $n in
                -p=*|--path=*)
                path="${n#*=}"
                ;;
                -f=*|--file=*)
                file="${n#*=}"
                ;;
                -l=*|--log=*)
                logfile="${n#*=}"
                ;;
        esac
        done

        if [ ! -d "$path" ];
        then 
               echo "Warc files path '$path' not found."
               echo "Exiting"
               exit
        fi

        if [ ! -f "$file" ];
        then 
               echo "nutchwax execution file '$file' not found."
               echo "Exiting"
               exit
        fi

        if [ ! -f "$logfile" ];
        then 
               echo "Log file '$logfile' not found."
               echo "Exiting"
               exit
        fi

        answer=`/bin/sh $file import_to_hdfs -p $path | tee -a $logfile`
        echo "$answer"
        if [ -z "$answer" -a "$answer" != " " ];
        then
                echo "All files already successfully processed. Exiting."
                exit
        fi

        echo "Successfully finished"
else
        echo "import_to_hdfs is running. Exiting"
fi
