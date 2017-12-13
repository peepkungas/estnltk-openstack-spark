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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TaggedIOException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;
import org.jwat.warc.WarcRecord;
import org.xml.sax.SAXException;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;


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


        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords = sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // TODO what should we do with the errors?
        // Extract text from Warc records using TIKA
        // If hbase is used then JavaRDD<Row> should be used
        JavaPairRDD<Text, Text> records2 = warcRecords
                .filter(new Function<Tuple2<LongWritable, WarcRecord>, Boolean>() {
                    public Boolean call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String header = "";

                       /*
                       Some WARC records do not have Content-Type header, such as
                       WARC-Type : resource
                       WARC-Target-URI : metadata://netarkivet.dk/crawl/setup/duplicatereductionjobs?majorversion=1&minorversion=0&harvestid=1&harvestnum=0&jobid=10
                       WARC-Date : 2015-02-15T21:23:35Z
                       WARC-Block-Digest : sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709
                       WARC-Warcinfo-ID : <urn:uuid:4916247c-8bfc-428d-b27d-4a28372dbf73>
                       WARC-IP-Address : 127.0.1.1
                       WARC-Record-ID : <urn:uuid:44b27d4f-06d8-46f9-9c18-441d173dd925>
                       Content-Length : 0
                       */

                        try {
                            header = s._2.getHeader("Content-Type").value;
                        } catch (NullPointerException e) {
                            return false;
                        }

                        // Ignore WARC and DNS files
                        if (header.equals("application/warc-fields")) return false;

                        if (header.equals("text/dns")) return false;

                        if (s._2.getHeader("WARC-Target-URI").value.startsWith("metadata")) return false;


                        return true;
                    }
                }).mapToPair(new PairFunction<Tuple2<LongWritable, WarcRecord>, Text, Text>() {
                    public Tuple2<Text, Text> call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String exceptionCause = "";

                        logger.debug("reading " + s._1);

                        // Construct the ID as it was in nutch, example:
                        // http::g.delfi.ee::/s/img/back_grey.gif::null::20150214090921
                        String date = s._2.getHeader("WARC-Date").value;
                        date = date.replaceAll("-|T|Z|:", "");


                        URL url = new URL(s._2.getHeader("WARC-Target-URI").value);
                        String protocol = url.getProtocol();
                        String hostname = url.getHost();
                        String urlpath = url.getPath();
                        String param = url.getQuery();

                        String id = protocol + "::" + hostname + "::" + urlpath + "::" + param + "::" + date;

                        // Ignore files that rarely have any meaningful text
                        List<String> ignoreList = new ArrayList<String>();
                        ignoreList.add(".css");
                        ignoreList.add(".js");
                        ignoreList.add("jquery");
                        ignoreList.add("robots.txt");

                        for (String suffix : ignoreList) {
                            if (urlpath.endsWith(suffix)) {
                                return new Tuple2<Text, Text>(new Text(id), new Text(""));
                            }
                        }

                        // Extract text from Warc file
                        try {
                            // Have Tika itself select the parser that matches content
                            AutoDetectParser parser = new AutoDetectParser();
                            // Minus 1 sets the limit to unlimited that is needed for bigger files
                            BodyContentHandler handler = new BodyContentHandler(-1);
                            Metadata metadata = new Metadata();

                            InputStream is = s._2.getPayload().getInputStream();

                            parser.parse(is, handler, metadata);

                            String out = removeHTMLTags(handler.toString());

                            logger.debug("finished " + s._1);

                            return new Tuple2<Text, Text>(new Text(id), new Text(out));

                        } catch (TikaException e) {
                            try {
                                exceptionCause = e.getCause().toString();
                            } catch (NullPointerException e1) {

                            }
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (SAXException e) {
                            try {
                                exceptionCause = e.getCause().toString();
                            } catch (NullPointerException e1) {

                            }
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (TaggedIOException e) {
                            // With new detection logic this happens
                        }

                        return new Tuple2<Text, Text>(new Text("EXCEPTION"), new Text(""));
                    }
                });

        saveSingleFolder(records2, outputPath, hadoopconf);

        sc.close();
    }


    private static void saveSingleFolder(JavaPairRDD<Text, Text> rdd, String outputPath, Configuration hadoopconf) {
        rdd.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
    }


    // Removes HTML tags from text
    // Some files have html text in them that are not parsed when text is extracted
    private static String removeHTMLTags(String text) {
        // https://stackoverflow.com/questions/5640334/how-do-i-preserve-line-breaks-when-using-jsoup-to-convert-html-to-plain-text
        Document document = Jsoup.parse(text);
        document.outputSettings(new Document.OutputSettings().prettyPrint(false));//makes html() preserve linebreaks and spacing
        document.select("br").append("\\n");
        document.select("p").prepend("\\n\\n");
        String s = document.html().replaceAll("\\\\n", "\n");
        return Jsoup.clean(s, "", Whitelist.none(), new Document.OutputSettings().prettyPrint(false));
        // Removes all double spaces AND \n
        // return Jsoup.parse(text).text();
    }
}
