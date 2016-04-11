Python program for executing ESTNLTK Name Entity Resolution.
Requires Spark libraries to be included in Python path.

Input : 
Output of spark_estnltk -ner process consisting of key-value pairs, where value is the processed HTML content of a web page, and list of the recognized entities. Key is used as an identifier.

Comma delimmited CSV file, which contains full list of name entities which should be recognized. 

- [inputPath] - Path to the folder/file containing output of spark_estnltk -ner process
- [csvFilePath] - Path to the file containing entity entries
- [outputPath] - Path to the output folder. Output path is optional. If output path is not specified results will be publised to out/out_<random_4_digit_nr>

spark-submit --master local[4] estlinkSpark.py [inputPath] [csvFilePath] [outputPath]

For the sample run lcoal files can be used.
spark-submit --master local[4] estlinkSpark.py part-00000 spark-input-example.csv

For automatic start use cron to call proccessing sequensfiles shell script (scripts/processEstlinkSparkStartup.sh <max_allowed_processes>).
All neede parameters (inputpath, Outputhpath, processed file and spark-submit paramters) are defined in script (scripts/processEstlinkSpark.sh).
    <max_allowed_processes> - If we have 9 cores in total, then 2 exections already use them all. If nothing specified 1 processes is default.

Example:
$ crontab -l
$ 0 * * * * /bin/sh /home/kaarelt/estnltk-openstack-spark/spark_NER/scripts/processEstlinkSparkStartup.sh

