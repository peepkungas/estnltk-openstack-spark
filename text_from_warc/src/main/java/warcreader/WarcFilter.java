package warcreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
//import jwat.warc.WarcRecord;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
public class WarcFilter implements Function<Tuple2<LongWritable, WarcRecord>, Boolean>, Serializable {

    private static final Logger logger = LogManager.getLogger(WarcFilter.class);

    private static String[] ignoreSuffix = new String[]{
            ".js", ".css", ".ttf", "jquery", ".gz", ".zip", ".7zip", "robots.txt"
    };


    public Boolean call(Tuple2<LongWritable, WarcRecord> s) {
        String header;
        String hostname;
        int size;

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
//            size = Integer.parseInt(s._2.getHeader("Content-Length").value);
            URL url = new URL(s._2.getHeader("WARC-Target-URI").value);
            hostname = url.getHost();

        } catch (NullPointerException e) {
            return false;
        } catch (MalformedURLException e) {
            return false;
        } catch (NumberFormatException e){
            return false;
        }

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return false;

        if (header.equals("text/dns")) return false;

        if (s._2.getHeader("WARC-Target-URI").value.startsWith("metadata")) return false;

        // only return pages that have response code 200 - OK
        try {
            if (s._2.getHttpHeader().statusCode != 200)
                return false;
        } catch (Exception e) {
            return false;
        }

        TupleObject idAndUrlpath = Utils.generateID(s._2);
        String id = idAndUrlpath.id;
        String urlpath = idAndUrlpath.urlpath;

        // Ignore files that rarely have any meaningful text
        for (String suffix : ignoreSuffix) {
            if (urlpath.endsWith(suffix)) {
                return false;
            }
        }

        // wordpress page that is used often but I have never encountered a good text from this:
        // contains only index of webpage, or images that webpage uses
        if (id.contains("/wp-content/")){
            return false;
        }
        // contains for example javascript code files
        if (id.contains("/wp-includes/")){
            return false;
        }
        if (id.contains("/css")){
            return false;
        }
        if (id.contains("/javascript")){
            return false;
        }

        // ignore content types from which no good text is extracted
        String httpHeader = "";
        try {
            httpHeader = s._2.getHttpHeader().getHeader("Content-Type").value;
            if (Utils.inList(httpHeader, Utils.ignoreContentTypes)) {
                return false;
            }
        } catch (Exception e) {
        }

        return true;
    }


}
