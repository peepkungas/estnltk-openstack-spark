This program matches names found by NER to a database of names and organizations.

# Usage

Three inputs are necessary:
* manifest file : file containing the full paths of each spark_NER output file.
* known entity data file : csv file containing person and organization data.
* output path : path to output directory. Each file specified in manifest creates a separate subdirectory.

Known entity data is loaded into memory, so executor memory size has to accomodate it.

# Execution
> spark-submit --conf "spark.executor.memory=4g" /home/kaarelt/NER_data_matching/nerMatcher.py /home/kaarelt/manifest.txt hdfs:///csv/spark-input-29122015.csv hdfs:///user/kaarelt/output/out43 



# Result grepping
> grep 'matches:\{[^\}]' outX/1/part-*
