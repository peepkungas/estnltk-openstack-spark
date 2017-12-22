package warcreader;

import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.function.Function;
import org.jwat.warc.WarcRecord;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by Madis-Karli Koppel on 13/12/2017.
 */
public class WarcTypeFilter implements Function<Tuple2<LongWritable, WarcRecord>, Boolean>, Serializable {

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

        // Ignore WARC specific content and DNS files
        if (header.equals("application/warc-fields")) return false;

        if (header.equals("text/dns")) return false;

        if (s._2.getHeader("WARC-Target-URI").value.startsWith("metadata")) return false;

        return true;
    }
}
