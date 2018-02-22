package warcreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
public class TextExtractorPairFunction implements PairFunction<Tuple2<LongWritable, WarcRecord>, Text, Text> {

    private static final Logger logger = LogManager.getLogger(TextExtractorPairFunction.class);

    public Tuple2<Text, Text> call(Tuple2<LongWritable, WarcRecord> s) throws MalformedURLException {
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
        ignoreList.add(".ttf");
        ignoreList.add("jquery");
        // 18/02/10 00:03:32 ERROR warcreader.TextExtractorPairFunction: OutOfMemoryError when parsing http::www.geenivaramu.ee::/tools/dbsnp37.txt.gz::null::20170820170215 Java heap space
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

            // TODO only parse non-html files here
            String out = removeHTMLTags(handler.toString());

            logger.debug("finished " + s._1);

            is.close();

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
        } catch (TaggedIOException e) {
            // With new detection logic this happens
        } catch (Exception e){
            logger.error("Unhandled exception when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        } catch (StackOverflowError e){
            logger.error("StackOverflowError when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        } catch (OutOfMemoryError e){
            logger.error("OutOfMemoryError when parsing " + id + " " + e.getMessage());
//            e.printStackTrace();
        } catch (Error e){
            logger.error("Unhandled error when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        }

        return new Tuple2<Text, Text>(new Text(id), new Text(""));
    }

    // Some non-HTML files have HTML tags in them that are not parsed when text is extracted
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
