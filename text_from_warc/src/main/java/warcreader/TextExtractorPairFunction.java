package warcreader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TaggedIOException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.html.HtmlMapper;
import org.apache.tika.sax.BodyContentHandler;
import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Parser;
import org.ccil.cowan.tagsoup.XMLWriter;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jwat.warc.WarcRecord;
import org.xml.sax.*;
import scala.Tuple2;

import java.io.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
// TODO refactor
public class TextExtractorPairFunction implements PairFunction<Tuple2<LongWritable, WarcRecord>, String, String> {

    private static final Logger logger = LogManager.getLogger(TextExtractorPairFunction.class);

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

            // Uncomment to use custom html parser
            // Add your own custom parsers by content type
//            Map<MediaType, org.apache.tika.parser.Parser> parsers = parser.getParsers();
//            parsers.replace(MediaType.text("html"), new CustomHtmlParser());
//            parsers.replace(MediaType.text("xhtml+xml"), new CustomHtmlParser());
//            parsers.replace(MediaType.text("vnd.wap.xhtml+xml"), new CustomHtmlParser());
//            parsers.replace(MediaType.text("x-asp"), new CustomHtmlParser());
//            parser.setParsers(parsers);

            // Minus 1 sets the limit to unlimited that is needed for bigger files
            BodyContentHandler handler = new BodyContentHandler(-1);
            // force encoding as most of the time tika wrongly detected html encoding
            Metadata metadata = new Metadata();
            metadata.add("Content-Encoding", "UTF-8");

            InputStream is = warcRecord.getPayload().getInputStream();

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

            String out = removeHTMLTags(extractedText);

            // ignore files that have more than 1% of their text as {
            int brackets = StringUtils.countMatches(out, "{");
            int total  = out.length();
            double percentage = brackets/(total * 1.0) * 100;

            if (percentage > 1.0){
                return "";
            }

            String out_cleaned = out.replaceAll("[^\\p{javaLetterOrDigit}\\p{Punct}\\s]", "").replaceAll("[ ]{2,}", " ").trim();

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

//            logger.error(textContent);
//            printstuff(id, tikaContentType, httpHeader, extractedText, out_cleaned);

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

    private static String removeHTMLTags(String contents) {
        Document doc = Jsoup.parse(contents);
        String textContent = doc.body().text();
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
