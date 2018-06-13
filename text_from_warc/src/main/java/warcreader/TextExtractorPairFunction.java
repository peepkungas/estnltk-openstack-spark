package warcreader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TaggedIOException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.safety.Whitelist;
//import jwat.warc.WarcRecord;
import org.jwat.warc.WarcRecord;
import org.xml.sax.*;
import scala.Tuple2;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
// TODO refactor
public class TextExtractorPairFunction implements PairFunction<Tuple2<LongWritable, WarcRecord>, String, String> {

    private static final Logger logger = LogManager.getLogger(TextExtractorPairFunction.class);

    private static final Charset ISO = Charset.forName("ISO-8859-1");
    private static final Charset UTF8 = Charset.forName("UTF-8");


    // TODO refactor
    public Tuple2<String, String> call(Tuple2<LongWritable, WarcRecord> tuple2) {

        TupleObject idAndUrlpath = Utils.generateID(tuple2._2);
        String id = idAndUrlpath.id;

        if (id == null)
            return new Tuple2(id, "");

        else if (id.length() > 1000)
            return new Tuple2(id, "");

        return new Tuple2(id, extractText(id, tuple2._2));
    }


    private static String extractText(String id, WarcRecord warcRecord){

        String exceptionCause = "";

        try {
            String httpHeader = "";

            try {
                httpHeader = warcRecord.getHttpHeader().getHeader("Content-Type").value;
            } catch (Exception e) {
                httpHeader = "Exception when extracting http header.";
            }

            // Have Tika itself select the parser that matches content
            AutoDetectParser parser = new AutoDetectParser();
            // Minus 1 sets the limit to unlimited that is needed for bigger files
            BodyContentHandler handler = new BodyContentHandler(-1);
            Metadata metadata = new Metadata();

            InputStream is = warcRecord.getPayload().getInputStream();

//            if(id.contains("valitsus.ee")){
//                String fixedHTML = fixHtml(IOUtils.toString(is));
//                is = IOUtils.toInputStream(fixedHTML);
//                logger.error(fixedHTML);
//            }

            parser.parse(is, handler, metadata);

            String tikaContentType = metadata.get("Content-Type");

            if (Utils.inList(tikaContentType, Utils.ignoreContentTypes)) {
                // this is required here for cases when we don't have a content-type from http header
                // or when the web page reports wrong Content-Type
                // for example http::www.feliximaja.ee::/show.php::pic=144::20180517175906
                // Tika says it is image/jpeg, the page says it is text/html
                return "";
            }

            // some webpages serve code files as octet-stream but tika detects it as text
            if(httpHeader.equals("application/octet-stream")){
                if (tikaContentType.contains("text/plain") || tikaContentType.contains("text/html")){
                    return "";
                }
            }


            String extractedText = handler.toString();

            String out = removeHTMLTags(extractedText, tikaContentType);

            // ignore files that have more than 1% of their text as {
            int brackets = StringUtils.countMatches(out, "{");
            int total  = out.length();
            double percentage = brackets/(total * 1.0) * 100;

//            if(id.contains("valitsus.ee")){
//                logger.error(percentage);
//                // 1035-1-20170821160701848-00002-ciblee_2015_netarchive.warc
//                printstuff(id, tikaContentType, httpHeader, extractedText, out);
//            }


            if (percentage > 1.0){
                return "";
            }

            // some json files with a lot of text don't fulfill this requirement
            // but they begin with { and end with }
            if (out.length() > 0) {
                if (out.trim().substring(0,1) == "{" || out.trim().substring(0,1) =="["){
                    logger.error("json stuff");
                    printstuff(id, tikaContentType, httpHeader, extractedText, out);
                    return "";
                }
            }

            String out_cleaned = out.replaceAll("\n|\\\\n|\t", " ").replaceAll("[ ]{2,}", " ").trim();

//            Set<String> uniqueWords = Arrays.stream(out_cleaned.split(" ")).map(word -> word.toLowerCase()).collect(Collectors.toSet());
////            if (uniqueWords.size() > 29 && uniqueWords.size() < 31){
//            if (uniqueWords.size() > 19 && uniqueWords.size() < 30){
//                if (uniqueWords.size() == 1 && uniqueWords.toArray()[0] != " "){
//
//                } else{
////                    System.out.println("\n" + uniqueWords.size() + " " + id);
////                    System.out.println(out_cleaned);
//                }
//            }

            is.close();

//            if(httpHeader.contains("binary/octet-stream")){
//                printstuff(id, tikaContentType, httpHeader, extractedText, out);
//            }



            return out_cleaned;

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
        } catch (Exception e) {
            logger.error("Unhandled exception when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        } catch (StackOverflowError e) {
            logger.error("StackOverflowError when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        } catch (OutOfMemoryError e) {
            logger.error("OutOfMemoryError when parsing " + id + " " + e.getMessage());
//            e.printStackTrace();
        } catch (Error e) {
            logger.error("Unhandled error when parsing " + id + " " + e.getMessage());
            e.printStackTrace();
        }
        return "";
    }

    private static String removeHTMLTags(String contents, String tikaContentType) {
        Document doc = Jsoup.parse(contents);
        String textContent = doc.text();
        return textContent;
    }

    private static void printstuff(String id, String tikaContentType, String httpHeader, String extractedText, String out){
        logger.error(id);
        logger.error("metadata content type from tika " + tikaContentType);
        logger.error("metadata content type from header " + httpHeader);
        logger.error("A " + extractedText.replaceAll("\n", " ").replaceAll("\t", " ").replaceAll("[ ]{2,}", " "));
        logger.error(StringUtils.repeat("-", extractedText.length()));
        logger.error("B " + out);
    }

    private static String fixHtml(String contents) {
        InputStream stream = new ByteArrayInputStream(contents.getBytes());

        HTMLSchema schema = new HTMLSchema();
        XMLReader reader = new Parser();

        try {
            reader.setProperty(Parser.schemaProperty, schema);
        } catch (SAXNotRecognizedException | SAXNotSupportedException e) {
            e.printStackTrace();
        }

        ByteArrayOutputStream out1 = new ByteArrayOutputStream(contents.getBytes().length + 100);
        Writer writeger = new OutputStreamWriter(out1);
        XMLWriter x = new XMLWriter(writeger);

        reader.setContentHandler(x);

        InputSource s = new InputSource(stream);
        String contents0 = "";
        try {
            reader.parse(s);
            contents0 = IOUtils.toString(new ByteArrayInputStream(out1.toByteArray()));
            contents = insertDoctypeFromSource(contents0, contents);
        } catch (Exception e) {
            return contents;
        }

        return contents;
    }

    protected static String insertDoctypeFromSource(String toBeReplacedHTML, String sourceHTML) {
        String oldDoctype = "";
        String currentHTMLHeader = "";
        String xmlHeader = "<?xml version=\"1.0\" standalone=\"yes\"?>";

        Pattern doctypePattern = Pattern.compile("<!DOCTYPE[^>]*>");
        Matcher doctypeMatcher = doctypePattern.matcher(sourceHTML);

        if (doctypeMatcher.find())
            oldDoctype = doctypeMatcher.group(0);

        if (oldDoctype.length() < 1)
            return toBeReplacedHTML;

        // White spaces are required between publicId and systemId.
        // <!DOCTYPE HTML PUBLIC ""> becomes <!DOCTYPE HTML PUBLIC "" "">
        String[] check = oldDoctype.replaceAll("\n", "").split("\"");

        // chekc 1 is ok
        if (check.length == 3) {
            // maybe not set the strict stuff
            oldDoctype = oldDoctype.substring(0, oldDoctype.length() - 1) + "  \"http://www.w3.org/\">";
        }

        Pattern htmlPattern = Pattern.compile("<html[^>]*>");
        Matcher currentHeaderMatcher = htmlPattern.matcher(toBeReplacedHTML);
        if (currentHeaderMatcher.find())
            currentHTMLHeader = currentHeaderMatcher.group(0);

        // <html> is length 6
        if (currentHTMLHeader.length() < 6) {
            toBeReplacedHTML = toBeReplacedHTML.replaceFirst(Pattern.quote(xmlHeader), xmlHeader + "\n" + oldDoctype);
            return toBeReplacedHTML;
        }

        return toBeReplacedHTML.replaceFirst(Pattern.quote(currentHTMLHeader), oldDoctype + "\n" + currentHTMLHeader);
    }
}
