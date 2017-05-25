Experimental/unfinished rework of the named entity to known entity matcher.

Example execution:

> spark-submit --conf "spark.executor.memory=16g" --conf "spark.kryoserializer.buffer.max=1024m" --conf "spark.executor.instances=4" --conf "spark.executor.cores=4" --py-files="nerMatcher/nerMatcherClasses.py" nerMatcher/nerMatcher.py -m benchmarking/manifest1.txt -k hdfs:/user/kaarelt/csv -x hdfs:/user/kaarelt/exclude -o hdfs:/user/kaarelt/benchmarking/test018