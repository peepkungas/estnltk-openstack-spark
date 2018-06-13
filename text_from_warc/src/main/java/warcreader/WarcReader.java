package warcreader;

import org.jwat.warc.WarcRecord;
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
//import jwat.warc.WarcRecord;

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

        // Increase Spark network timeout from default 120s
//        sparkConf.set("spark.network.timeout", "1000000");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("ERROR");

        Configuration hadoopconf = new Configuration();

        // against java.io.IOException: Filesystem closed
        hadoopconf.setBoolean("fs.hdfs.impl.disable.cache", true);

        // TODO convert jwat.warcreader to use String so that it would be serializable
        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords =
                sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        JavaPairRDD<LongWritable, WarcRecord> filteredWarcRecords = warcRecords.filter(new WarcFilter());

//
//        // make the thing serializable
//        JavaRDD<Tuple4<String, String, String, String>> serializableWarcRecords = filteredWarcRecords.map(line -> new Tuple4(
//                line._2.getHeader("WARC-Target-URI").value,
//                line._2.getHeader("WARC-Date").value,
//                line._2.getHeader("Content-Type").value,
//                IOUtils.toString(line._2.getPayload().getInputStream())));

//        serializableWarcRecords.repartition(200);

//        for(Tuple4 line: serializableWarcRecords.take(5)){
//            System.out.println(line);
//        }


        // Extract text from Warc records using TIKA
        JavaPairRDD<String, String> extractedText = filteredWarcRecords
                .mapToPair(new TextExtractorPairFunction());


        JavaPairRDD<String, String> noEmptyText = extractedText
                .filter(line -> !line._2.trim().isEmpty());

//        JavaPairRDD<String, Integer> keyCounts = extractedText
//                .mapToPair(x -> new Tuple2<String, Integer>(x._1,1)).reduceByKey((x, y) -> x + y).sortByKey();
//
//
//        for(Tuple2 line: noEmptyText.collect()){
////            System.out.println(line);
//        }

        JavaPairRDD<Text, Text> output = noEmptyText.mapToPair(line -> new Tuple2(new Text(line._1), new Text(line._2)));

        saveSingleFolder(output, outputPath, hadoopconf);
//        noEmptyText.saveAsObjectFile(outputPath);

        sc.close();
    }

    private static void saveSingleFolder(JavaPairRDD<Text, Text> rdd, String outputPath, Configuration hadoopconf) {
        rdd.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class,
                org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
    }

}
