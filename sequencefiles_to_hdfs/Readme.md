This program moves from WARC archive generated Hadoop SequenceFiles files into HDFS.
This is a helper project for getting SequenceFiles from local to HDFS since warc_to_hdfs project has hadoop version upgrade problems (if server version is newer than client version, warc_to_hdfs can not write SequenceFiles to HDFS).

Build command:

    mvn clean install

Usage:
    Generate jar file with mvn.
    After generating jar file use:
    
    java -jar <generated_jar_file> <sequence_files_inputpath> <sequence_files_outputpath>
    <sequence_files_outputpath> can be local file system or hdfs (for hdfs use: hdfs://<hadoop_server>:<port>/<output_path> 
    example: java -jar /home/kaarelt/sequencefiles-to-hdfs-0.0.1-SNAPSHOT.jar /home/kaarelt/sequencefiles/ hdfs://hadoop-ner-1:8020/sequencefiles

For automatic start use cron to call sqeuencefiles_to_hdfs shell script (scripts/move_sequencefiles_files_to_hdfs.sh)
    -f=<jar_file_location>
    -p=<log_file_properties>
    -i=<sequence_files_inputpath>
    -o=<sequence_files_outputpath>

Example:
$ crontab -l
$ 0 * * * * /bin/sh /home/kaarelt/estnltk-openstack-spark/sequencefiles_to_hdfs/scripts//move_sequencefiles_files_to_hdfs.sh -f=/home/kaarelt/sequencefiles-to-hdfs-0.0.1-SNAPSHOT.jar -p= /home/kaarelt/estnltk-openstack-spark/sequencefiles_to_hdfs/src/main/resources/log4j.properties -i=/home/kaarelt/sequencefiles -o=hdfs://hadoop-ner-1:8020/sequencefiles

