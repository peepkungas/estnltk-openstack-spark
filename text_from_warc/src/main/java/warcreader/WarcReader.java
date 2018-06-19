package warcreader;

import scala.Tuple2;
//import warcutils.WarcInputFormat;
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
//        sparkWarcReader(inputPath, outputPath);
        logger.info("Spark Finished...");
        long end = System.currentTimeMillis();
        logger.error("Total time taken " + (end - start));

    }

    /*
     * Extracts text content from Warc files
     * inputPath - path to warc file
     * outputPath - path where sequence files are outputted
    */
    private static void sparkWarcReader(String inputPath, String outputPath) {

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Warc text extractor");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        Configuration hadoopconf = new Configuration();

        // against java.io.IOException: Filesystem closed
        hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);

        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords =
                sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        JavaPairRDD<LongWritable, WarcRecord> filteredWarcRecords = warcRecords
                .filter(new WarcFilter());

        // Extract text from Warc records using TIKA
        JavaPairRDD<String, String> extractedText = filteredWarcRecords
                .mapToPair(new TextExtractorPairFunction());


        JavaPairRDD<String, String> noEmptyText = extractedText
                .filter(line -> !line._2.trim().isEmpty());

        JavaPairRDD<Text, Text> output = noEmptyText
                .mapToPair(line -> new Tuple2(new Text(line._1), new Text(line._2)));

        output.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class,
                org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);

        sc.close();
    }

}
