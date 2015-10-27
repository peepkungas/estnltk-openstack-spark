Python program for executing ESTNLTK operations on web page SequenceFiles.
Requires ESTNLTK and Spark libraries to be included in Python path.

Applicable arguments to perform ESTNLTK processes. Including each one creates a separate corresponding output folder. Non-included results are not included in output, even if performed.
    -token : Tokenization (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#tokenization). Paragraph tokenization is performed for sentence tokenization and sentence tokenization is performed for word tokenization.
    	-tokenLevel=<word,sentence,paragraph> : When tokenizing, apply the chosen level of granularity (word, sentence or paragraph). Defaults to paragraph.
	-lemma : Lemmatization (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -postag : Part-of-speech (POS) tagging (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -morph : Morphological analysis (http://estnltk.github.io/estnltk/1.3/tutorials/text.html#morphological-analysis). Performs word tokenization, if not performed before.
    -ner : Named entity recognition (http://estnltk.github.io/estnltk/1.3/tutorials/ner.html). Performs tokenization and morphological analysis, if not performed before.
	-all : Applies all of the processes listed above and combines them into one output.
	
Other arguments:
	-includeBoilerplate : Includes text obtained from boilerplate paragraphs. Defaults to False.
	
Input : Hadoop SequenceFile (or folder containing SequenceFiles) consisting of key-value pairs, where value is the unprocessed HTML content of a web page. Key is used as an identifier.
Output : Folder containing folders, each with the output of an applied process.

Usage with spark submit (master 4): spark-submit --master[4] process.py file://<inputpath> file://<outputpath> [<processnames>] 

Usage without submit : python process.py <inputpath> <outputpath> [<processnames>]
e.g : python process.py input/myseqfile1 out1 -token -lemma

Known issues:
	* Running without spark-submit may crash when writing to files (socket closed error, pyspark issue)