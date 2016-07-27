This program matches names found by NER to a database of names and organizations.

<h1>Usage<\h1>

Three inputs are necessary:
* manifest file : file containing the full paths of each spark_NER output file.
* known entity data file : csv file containing person and organization data.
* output path : path to output directory. Each file specified in manifest creates a separate subdirectory.

Known entity data is loaded into memory, so executor memory size has to accomodate it.

<h1>Execution<\h1>
> spark-submit --conf "spark.executor.memory=4g" /home/kaarelt/NER_data_matching/nerMatcher.py /home/kaarelt/manifest.txt hdfs:///csv/spark-input-29122015.csv hdfs:///user/kaarelt/output/out43 



<h1>Result grepping<\h1>
> grep 'matches:\{[^\}]' outX/1/part-*
