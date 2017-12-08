package warcreader;

import com.typesafe.config.ConfigException;
import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TaggedIOException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;
import org.jwat.common.HeaderLine;
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
        //sparkWarcReader(inputPath, outputPath, doOutputSeparation);
        sparkWarcReader(inputPath, outputPath + System.currentTimeMillis());
        logger.info("Spark Finished...");
        long end = System.currentTimeMillis();
        logger.error("Total time taken " + (end - start));

    }

    /*  Extracts text content from warc files
     * inputPath - path to warc file
     * outputPath - path where sequence files are outputted
     * doOutputSeparation - if true then output is separated into several folders based on Content-Type
    */
    private static void sparkWarcReader(String inputPath, String outputPath) {

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Spark PDF text extraction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // SQLContext sqlContext = new SQLContext(sc);

        Configuration hadoopconf = new Configuration();


        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords = sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);

        // TODO what should we do with the errors?
        // Extract text from Warc records using TIKA
        // If hbase is used then JavaRDD<Row> should be used
        JavaPairRDD<Text, Text> records2 = warcRecords
                .filter(new Function<Tuple2<LongWritable, WarcRecord>, Boolean>() {
                    public Boolean call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String header = s._2.getHeader("Content-Type").value;

                        // Ignore WARC and DNS files
                        if (header.equals("application/warc-fields")) return false;

                        if (header.equals("text/dns")) return false;

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
                            // Minus 1 sets the limit to unlimited
                            // This is needed for bigger files
                            BodyContentHandler handler = new BodyContentHandler(-1);
                            Metadata metadata = new Metadata();

                            InputStream is = s._2.getPayload().getInputStream();

                            parser.parse(is, handler, metadata);

                            // Remove all remaining HTML tags that were not parsed yet.
                            String out = removeHTMLTags(handler.toString());

                            logger.debug("finished " + s._1);

                            return new Tuple2<Text, Text>(new Text(id), new Text(out));

                        } catch (TikaException e) {
                            try {
                                exceptionCause = e.getCause().toString();
                            } catch (NullPointerException e1) {

                            }
                            logger.error(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (SAXException e) {
                            try {
                                exceptionCause = e.getCause().toString();
                            } catch (NullPointerException e1) {

                            }
                            logger.error(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (NoSuchMethodError e) {
                            // A small hack until
                            // Caused by: java.lang.NoSuchMethodError: org.apache.commons.compress.compressors.CompressorStreamFactory.setDecompressConcat                                                        enated(Z)V
                            // at org.apache.tika.parser.pkg.CompressorParser.parse(CompressorParser.java:102)
                            // is resolved
                            exceptionCause = "UNK Cause";
                            logger.error(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);

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
        //contents.coalesce(1).saveAsNewAPIHadoopFile(outputPath, String.class, Integer.class, org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class, hadoopconf);
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

    // Method for saving RowRDD, not used currently
//    private static void saveDF(JavaRDD<Row> javaRDD, SQLContext sqlContext, String outputPath, String dirName) {
//        List<StructField> fields = new ArrayList<StructField>();
//        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("contentType", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("content", DataTypes.StringType, true));
//        StructType schema = DataTypes.createStructType(fields);
//
//        DataFrame df = sqlContext.createDataFrame(javaRDD, schema);
//
//        // TODO save as into Hbase
//    }

//    public static void readHbase() throws IOException {
//        // https://community.hortonworks.com/articles/2038/how-to-connect-to-hbase-11-using-java-apis.html
//        logger.info("Setting up hbase configuration");
//        TableName tableName = TableName.valueOf("stock-prices");
//
//        Configuration conf = HBaseConfiguration.create();
//        // Clientport 2181, but docker changes it
//        conf.set("hbase.zookeeper.property.clientPort", "32779");
//        conf.set("hbase.zookeeper.quorum", "172.17.0.2");
//        conf.set("zookeeper.znode.parent", "/hbase");
//        logger.info("Connecting to hbase");
//        Connection conn = ConnectionFactory.createConnection(conf);
//        logger.info("Connected");
//        Admin admin = conn.getAdmin();
////        if (!admin.tableExists(tableName)) {
//        admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("cf")));
////        }
//        logger.info("Inserting into table");
//        Table table = conn.getTable(tableName);
//        Put p = new Put(Bytes.toBytes("AAPL10232015"));
//        p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("close"), Bytes.toBytes(119));
//        table.put(p);
//        logger.info("Inserted");
//
//        logger.info("Reading from table");
//        Result r = table.get(new Get(Bytes.toBytes("AAPL10232015")));
//        System.out.println(r);
//    }
}


