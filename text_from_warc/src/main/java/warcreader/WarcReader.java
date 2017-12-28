package warcreader;

import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.jwat.warc.WarcRecord;

import java.io.IOException;

// TODO IGNORE METADATA FILES!
// TODO what should we do with the errors?

/**
 * Created by Madis-Karli Koppel on 3/11/2017.
 * Reads warc files and extracts textual content
 * Ignores some warc records, such as DNS
 * Ignores code files that contain no good text
 */
public class WarcReader {

    private static final Logger logger = LogManager.getLogger(WarcReader.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: WarcReader <input folder> <output folder>");
            throw new IOException("Wrong input arguments");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        long start = System.currentTimeMillis();
        logger.info("Starting spark...");
        sparkWarcReader(inputPath, outputPath + System.currentTimeMillis());
        logger.info("Spark Finished...");
        long end = System.currentTimeMillis();
        logger.error("Total time taken " + (end - start));

    }

    /*
     * Extracts text content from warc files
     * inputPath - path to warc file
     * outputPath - path where sequence files are outputted
    */
    private static void sparkWarcReader(String inputPath, String outputPath) {

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Warc text extractor");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Configuration hadoopconf = new Configuration();

        // against java.io.IOException: Filesystem closed
        hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);

        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords =
                sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // Extract text from Warc records using TIKA
        // If hbase is used then JavaRDD<Row> should be used
        JavaPairRDD<Text, Text> records2 = warcRecords
                .filter(new WarcTypeFilter())
                .mapToPair(new TextExtractorPairFunction());

        saveSingleFolder(records2, outputPath, hadoopconf);

        sc.close();
    }

    private static void saveSingleFolder(JavaPairRDD<Text, Text> rdd, String outputPath, Configuration hadoopconf) {
        rdd.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
    }

}
