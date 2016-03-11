Java program for converting plaintext files into Hadoop Sequencefiles.

Execution:
    java  -Dlog4j.configuration=file:<log4j_properties_file_location> -jar <text_to_hdfs_jar_file_location> <inputfilepath> <outputfilepath>

    <text_to_hdfs_jar_file_location> : filesystem path of the text to hdfs jar file 
    <log4j_properties_file_location> : filesystem path of the log4j properties file
    <inputfilepath> : filesystem path of the input text file.
    <outfilepath> : filesystem (also hdfs filesystem) path to the output directory. The output file name will be the input file name appended with ".seq". For using output to hdfs filesystem add hdfs fullpath with server name, port and direcotry (eg. hdfs://<server>:<port>/<outputpath>) 

Example 1 (to local disk):
Let's have an input text file at "file:/home/ubuntu/kaarelt/example/mytextfile". Then we will use the command:
    java -Dlog4j.configuration=file:/home/ubuntu/kaarelt/log4j.properties -jar /home/ubuntu/kaarelt/text_to_hdfs/text_to_hdfs.jar file:/home/ubuntu/kaarelt/example/mytextfile file:/home/ubuntu/kaarelt/outexample
This will create "mytextfile.seq" into the directory "outexample".

Example 2 (to hdfs):
Let's have an input text file at "file:/home/ubuntu/kaarelt/example/mytextfile". Then we will use the command:
    java -Dlog4j.configuration=file:/home/ubuntu/kaarelt/log4j.properties -jar /home/ubuntu/kaarelt/text_to_hdfs/text_to_hdfs.jar file:/home/ubuntu/kaarelt/example/mytextfile hdfs://hadoop-ner-1:8020/user/ubuntu/outexample/
This will create "mytextfile.seq" into the directory "/user/ubuntu/outexample" in hdfs server "hdfs://hadoop-ner-1:8020".

