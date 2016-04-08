Python program for executing Estnltk operations on SequenceFiles.

Dependencies (needed to be included in Python path):
    Estnltk https://github.com/estnltk/estnltk
    jusText https://github.com/miso-belica/jusText
    Spark Python libraries (PySpark)
    
Arguments:
Applicable arguments to perform ESTNLTK processes. The results of non-included processes are not included in output, even if the process is performed.
    -token : Perform tokenization (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#tokenization). Paragraph tokenization is performed for sentence tokenization and sentence tokenization is performed for word tokenization.
    -tokenLevel=<word,sentence,paragraph> : When tokenizing, apply the chosen level of granularity (word, sentence or paragraph). Defaults to paragraph.
    -lemma : Perform lemmatization (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -postag : Perform part-of-speech (POS) tagging (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -morph : Perform morphological analysis (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -ner : Perform named entity recognition (http://estnltk.github.io/estnltk/1.3/tutorials/ner.html). Performs tokenization and morphological analysis, if not performed before.
    -all : Applies all of the processes listed above.
Other arguments:
    -includeBoilerplate : Includes text obtained from boilerplate paragraphs (low importance HTML paragraphs). Defaults to False.
    -isPlaintextInput : Indicates that SequenceFiles contain ordinary text, not HTML, therefore no HTML cleaning/removal is performed. Defaults to False.
    
Input : Hadoop SequenceFile (or folder containing SequenceFiles) consisting of key-value pairs.
    The key is used as an identifier and can be arbitrary. For HTML, it is usually the URL of the page.
    The value is the unprocessed HTML content of a web page (or with -isPlaintextInput, any text). 
Output : Directory containing the results of the process. One result file is created for each input file. Each row starts with a SequenceFile key and contains the results obtained from processing the corresponding SequenceFile value.


Usage with spark submit (master 4) : spark-submit --master[4] process.py file://<inputpath> file://<outputpath> [<processnames>] 
Usage without submit : python process.py <inputpath> <outputpath> [<processnames>]

Example:
Let's have a YARN cluster with 4 executor nodes. We have three sequencefiles in "hdfs:/kaarelt/seqfiles" and wish to process them into "hdfs:/kaarelt/processresults". We wish to perform lemmatization and named entity recognition.
1) Execute the command (where "/home/kaarelt/process.py" is the location of the spark_estnltk script):
    spark-submit --master yarn-cluster --num-executors 4 /home/kaarelt/process.py hdfs:/kaarelt/seqfiles hdfs:/kaarelt/processresults -lemma  -ner
This will process the sequencefiles and output the results into "hdfs:/kaarelt/processresults".
    
Known issues:
    * Running without spark-submit may crash when writing to files (socket closed error, pyspark issue)

For automatic start use cron to call proccessing sequensfiles shell script (scripts/processSeqFilesStartup.sh <max_allowed_processes>).
All neede parameters (inputpath, Outputhpath, processed file and spark-submit paramters) are defined in script (scripts/processSequencefiles.sh).
    <max_allowed_processes> - If we have 9 cores in total, then 2 exections already use them all. If nothing specified 1 processes is default.

Example:
$ crontab -l
$ 0 * * * * /bin/sh /home/kaarelt/estnltk-openstack-spark/spark_estnltk/spark_estnltk/scripts/processSeqFilesStartup.sh
