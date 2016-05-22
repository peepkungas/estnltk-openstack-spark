/* 
 * Kaarel T\u00F5nisson 2015.
 */

package org.apache.nutch.parse.warctohdfs;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.w3c.dom.DocumentFragment;

/**
 * Convert WARC file contents into HDFS SequenceFiles in format key:
 * domain::path::date value: HTML content of page
 */
public class WarcToHdfsParseFilter implements HtmlParseFilter {

    private static final Log LOG = LogFactory.getLog(WarcToHdfsParseFilter.class.getName());

    private Configuration conf;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
        Parse parse = parseResult.get(content.getUrl());
        Metadata metadata = parse.getData().getParseMeta();

        byte[] contentInOctets = content.getContent();
        String htmlraw = new String();

        // try getting content encoding
        try {
            htmlraw = new String(contentInOctets, metadata.get("OriginalCharEncoding"));
        } catch (UnsupportedEncodingException e) {
            LOG.warn("could not get content with OriginalCharEncoding");
        }

        // if unable, try utf-8
        if (htmlraw.length() == 0) {
            try {
                htmlraw = new String(contentInOctets, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                LOG.error("unable to convert content into string");
            }
        }

        /*
         * // meta to consider or try : metadata.CONTENT_LOCATION,
         * metadata.DATE, metadata.FETCH_TIME_KEY, metadata.LAST_MODIFIED //
         * meta only contains char encodings LOG.info("Metadata count: " +
         * metadata.names().length); for (String name : metadata.names()){
         * LOG.info("meta " + name + " : " + metadata.get(name)); }
         */

        URL url = null;
        try {
            url = new URL(content.getUrl());
        } catch (MalformedURLException e) {
            LOG.error("Malformed URL Exception: " + e.getMessage());
        }
        
        String protocol = url.getProtocol();
        String hostname = url.getHost();
        String urlpath = url.getPath();
        LOG.info("PROTOCOL:" + protocol);
        LOG.info("HOST:" + hostname);
        LOG.info("PATH:" + urlpath);

        // TODO: date is not found
        LOG.info("meta date: " + metadata.GENERATE_TIME_KEY);
        String date = metadata.get("DATE_KEY");
        LOG.info("meta date B : " + date);

        SequenceFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            // TODO: replace with sensible filename (date maybe?)
            Random randomGenerator = new Random();
            Path path = new Path("./sequencefiles/sf_" + randomGenerator.nextInt(Integer.MAX_VALUE));
            Text key = new Text();
            Text value = new Text();
            key.set(protocol + "::" + hostname + "::" + urlpath + "::" + date);
            value.set(htmlraw);
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
            LOG.info("len: " + writer.getLength() + ", key: " + key + ", value len: " + value.getLength());
            writer.append(key, value);
            writer.close();
        } catch (IOException e) {
            LOG.error("SequenceFile IOException: " + e.getMessage());
        }

        return parseResult;
    }

}
