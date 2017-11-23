import nl.surfsara.warcutils.WarcInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.tika.exception.TikaException;
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
 * Also ignores CSS and JS files that contain no good text
 */
public class WarcReader {

    private static final Logger logger = LogManager.getLogger(WarcReader.class);

    public static void main(String[] args) throws IOException {

        if (args.length < 2) {
            System.err.println("Usage: WarcReader <input folder> <output folder>");
            System.err.println("Additional 3rd arg - if you want separate output folders for html, plaintext and others");
            throw new IOException("Wrong input arguments");
        }

        String inputPath = args[0];
        String outputPath = args[1];

        boolean doOutputSeparation = false;
        try {
            String thirdArg = args[3];
            doOutputSeparation = true;
        } catch (ArrayIndexOutOfBoundsException e) {

        }

        long start = System.currentTimeMillis();
        logger.info("Starting spark...");
        sparkWarcReader(inputPath, outputPath, doOutputSeparation);
        logger.info("Spark Finished...");
        long end = System.currentTimeMillis();
        logger.error("Total time taken " + (end - start));

        //readHbase();

    }

    /*  Extracts text content from warc files
     * inputPath - path to warc file
     * outputPath - path where sequence files are outputted
     * doOutputSeparation - if true then output is separated into several folders based on Content-Type
    */
    private static void sparkWarcReader(String inputPath, String outputPath, boolean doOutputSeparation) {

        // Initialise Spark
        SparkConf sparkConf = new SparkConf().setAppName("Spark PDF text extraction");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        // SQLContext sqlContext = new SQLContext(sc);

        Configuration hadoopconf = new Configuration();


        //Read in all warc records
        JavaPairRDD<LongWritable, WarcRecord> warcRecords = sc.newAPIHadoopFile(inputPath, WarcInputFormat.class, LongWritable.class, WarcRecord.class, hadoopconf);


        // Extract text from Warc records using TIKA
        // If hbase is used then JavaRDD<Row> should be used
        JavaPairRDD<Text, Tuple2<Text, Text>> records2 = warcRecords
                .filter(new Function<Tuple2<LongWritable, WarcRecord>, Boolean>() {
                    public Boolean call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String header = s._2.getHeader("Content-Type").value;

                        // Ignore WARC and DNS files
                        if (header.equals("application/warc-fields")) return false;

                        if (header.equals("text/dns")) return false;

                        return true;
                    }
                }).mapToPair(new PairFunction<Tuple2<LongWritable, WarcRecord>, Text, Tuple2<Text, Text>>() {
                    public Tuple2<Text, Tuple2<Text, Text>> call(Tuple2<LongWritable, WarcRecord> s) throws Exception {
                        String exceptionMessage = "";
                        String exceptionCause = "";
                        String contentType = "";

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

                        for(String suffix: ignoreList){
                            if (urlpath.endsWith(suffix)) {
                                return new Tuple2<Text, Tuple2<Text, Text>>(new Text("IGNORE"), new Tuple2<Text, Text>(new Text(id), new Text("")));
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

                            contentType = metadata.get("Content-Type");

                            // Remove all remaining HTML tags that were not parsed yet.
                            String out = removeHTMLTags(handler.toString());

                            logger.debug("finished " + s._1);

                            return new Tuple2<Text, Tuple2<Text, Text>>(new Text(contentType), new Tuple2<Text, Text>(new Text(id), new Text(out)));

                        } catch (TikaException e) {
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getCause().toString();
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        } catch (SAXException e) {
                            exceptionMessage = e.getMessage();
                            exceptionCause = e.getException().toString();
                            logger.debug(e.getMessage() + " when parsing " + id + " cause " + exceptionCause);
                        }

                        // TODO what should we do with the errors?
                        //return new Tuple2<Text, Tuple2<Text, Text>>(new Text("ERROR" + contentType), new Tuple2<Text, Text>(new Text(id), new Text("ERROR: " + exceptionMessage + ":" + exceptionCause + " | " + Arrays.asList(s._2.getHeaderList()))));
                        return new Tuple2<Text, Tuple2<Text, Text>>(new Text("ERROR" + contentType), new Tuple2<Text, Text>(new Text(id), new Text("")));
                    }
                });

        if (doOutputSeparation) {
            saveMultiFolder(records2, outputPath, hadoopconf);
        } else {
            saveSingleFolder(records2, outputPath, hadoopconf);
        }

        sc.close();
    }

    // Save RDD into only one folder
    private static void saveSingleFolder(JavaPairRDD<Text, Tuple2<Text, Text>> rdd, String outputPath, Configuration hadoopconf) {
        JavaPairRDD<Text, Text> output = rdd.mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                String pure = Jsoup.parse(s._2._2.toString()).text();
                return new Tuple2<Text, Text>(s._2._1, new Text(pure));
            }
        });

        output.saveAsNewAPIHadoopFile(outputPath, Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
    }

    // Separete RDD into multiple folters:
    // /html - html and html like formats (xml, xhtml, rss+xml)
    // /others
    // /plaintext - only plaintext
    private static void saveMultiFolder(JavaPairRDD<Text, Tuple2<Text, Text>> rdd, String outputPath, Configuration hadoopconf) {
        // filter based on file type. Needed for testing and can be removed later on
        // Html and html-like files
        JavaPairRDD<Text, Text> html = rdd.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/html")) return true;
                if (s._1.toString().contains("application/xhtml+xml")) return true;
                if (s._1.toString().contains("application/xml")) return true;
                if (s._1.toString().contains("application/rss+xml")) return true;
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        // plaintext files, mostly robot responses or javascript code
        JavaPairRDD<Text, Text> plaintext = rdd.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/plain")) return true;
                return false;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        // all other file types
        // includes pdf and word documents
        JavaPairRDD<Text, Text> others = rdd.filter(new Function<Tuple2<Text, Tuple2<Text, Text>>, Boolean>() {
            public Boolean call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                if (s._1.toString().contains("text/plain")) return false;
                if (s._1.toString().contains("text/html")) return false;
                if (s._1.toString().contains("application/xhtml+xml")) return false;
                if (s._1.toString().contains("application/xml")) return false;
                if (s._1.toString().contains("application/rss+xml")) return false;
                return true;
            }
        }).mapToPair(new PairFunction<Tuple2<Text, Tuple2<Text, Text>>, Text, Text>() {
            public Tuple2<Text, Text> call(Tuple2<Text, Tuple2<Text, Text>> s) throws Exception {
                return s._2;
            }
        });

        html.saveAsNewAPIHadoopFile(outputPath + "/html", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        plaintext.saveAsNewAPIHadoopFile(outputPath + "/plaintext", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        others.saveAsNewAPIHadoopFile(outputPath + "/others", Text.class, Text.class, org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat.class, hadoopconf);
        ///saveDF(html, sqlContext, outputPath, "html");
        //saveDF(plaintext, sqlContext, outputPath, "plaintext");
        //saveDF(others, sqlContext, outputPath, "others");
    }

    // Removes HTML tags from text
    // however, when run on PDF output then line breaks and double spaces are lost.
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
    private static void saveDF(JavaRDD<Row> javaRDD, SQLContext sqlContext, String outputPath, String dirName) {
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("contentType", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("content", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);

        DataFrame df = sqlContext.createDataFrame(javaRDD, schema);

        // TODO save as into Hbase
    }

    public static void readHbase() throws IOException {
        // https://community.hortonworks.com/articles/2038/how-to-connect-to-hbase-11-using-java-apis.html
        logger.info("Setting up hbase configuration");
        TableName tableName = TableName.valueOf("stock-prices");

        Configuration conf = HBaseConfiguration.create();
        // Clientport 2181, but docker changes it
        conf.set("hbase.zookeeper.property.clientPort", "32779");
        conf.set("hbase.zookeeper.quorum", "172.17.0.2");
        conf.set("zookeeper.znode.parent", "/hbase");
        logger.info("Connecting to hbase");
        Connection conn = ConnectionFactory.createConnection(conf);
        logger.info("Connected");
        Admin admin = conn.getAdmin();
//        if (!admin.tableExists(tableName)) {
        admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor("cf")));
//        }
        logger.info("Inserting into table");
        Table table = conn.getTable(tableName);
        Put p = new Put(Bytes.toBytes("AAPL10232015"));
        p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("close"), Bytes.toBytes(119));
        table.put(p);
        logger.info("Inserted");

        logger.info("Reading from table");
        Result r = table.get(new Get(Bytes.toBytes("AAPL10232015")));
        System.out.println(r);
    }
}


