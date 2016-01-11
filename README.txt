This repository contains programs for applying Estnltk language processing to text content using Spark parallel execution.  

Contents:
spark_estltk : Python process for applying Estnltk processes to text content (or HTML pages) contained in Sequencefiles. Created for Spark parallel execution.
text_to_hdfs : Java program for converting plaintext into Hadoop SequenceFiles.
warc_to_hdfs : NutchWAX-based program for converting WARC (Web page ARChive) files into SequenceFiles.


Getting started:
	0. See readme files of each component for installation and usage instructions.
	1. Obtain SequenceFiles for processing:
		1a For text files, use text_to_hdfs to convert them into SequenceFiles
		1b For WARC files, use warc_to_hdfs to convert them into SequenceFiles
	2. Use spark_estnltk to process the SequenceFiles and obtain language analysis results