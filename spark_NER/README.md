Python program for executing ESTNLTK Name Entity Resolution.
Requires Spark libraries to be included in Python path.

Input : 
Output of spark_estnltk -ner process consisting of key-value pairs, where value is the processed HTML content of a web page, and list of the recognized entities. Key is used as an identifier.

Comma delimmited CSV file, which contains full list of name entities which should be recognized. 

- [inputPath] - Path to the folder/file containing output of spark_estnltk -ner process
- [csvFilePath] - Path to the file containing entity entries

spark-submit --master local[4] estlinkSpark.py [inputPath] [csvFilePath]

For the sample run lcoal files can be used.
spark-submit --master local[4] estlinkSpark.py part-00000 spark-input-example.csv




