/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.archive.nutchwax;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.crawl.SignatureFactory;
import org.apache.nutch.fetcher.FetcherOutputFormat;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.net.URLFilterException;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ProtocolStatus;
import org.apache.nutch.scoring.ScoringFilters;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;
import org.apache.nutch.util.StringUtil;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;

/**
 * Import Archive files (.arc/.warc) files into a newly-created Nutch segment.
 *
 * <code>Importer</code> is coded as a Hadoop job and is intended to be run
 * within the Hadoop framework, or at least started by the Hadoop launcher
 * incorporated into Nutch. Although there is a <code>main</code> driver, the
 * Nutch launcher script is strongly recommended.
 *
 * This class was initially adapted from the Nutch <code>Fetcher</code> and
 * <code>ArcSegmentCreator</code> classes. The premise is since the Nutch
 * fetching process acquires external content and places it in a Nutch segment,
 * we can perform a similar activity by taking content from the ARC files and
 * place that content in a Nutch segment in a similar fashion. Ideally, once the
 * <code>Importer</code> is used to import a set of ARCs into a Nutch segment,
 * the resulting segment should be more-or-less the same as one created by
 * Nutch's own Fetcher.
 * 
 * Since we are mimicing the Nutch Fetcher, we have to be careful about some
 * implementation details that might not seem relevant to the importing of ARC
 * files. I've noted those details with comments prefaced with "?:".
 */
public class ImporterToHdfs extends Configured implements Tool,
        Mapper<WritableComparable, Writable, Text, NutchWritable> {

    public static final Log LOG = LogFactory.getLog(ImporterToHdfs.class);

    private JobConf jobConf;
    private URLFilters urlFilters;
    private ScoringFilters scfilters;
    private ParseUtil parseUtil;
    private URLNormalizers normalizers;
    private int interval;
    private HTTPStatusCodeFilter httpStatusCodeFilter;

    private String seqFilePrefix;
    private String seqFileSuffix;

    private String seqFilePath;

    /**
     * ?: Is this necessary?
     */
    public ImporterToHdfs() {

    }

    /**
     * <p>
     * Constructor that sets the job configuration.
     * </p>
     * 
     * @param conf
     */
    public ImporterToHdfs(Configuration conf) {
        setConf(conf);
    }

    /**
     * <p>
     * Configures the job. Sets the url filters, scoring filters, url
     * normalizers and other relevant data.
     * </p>
     * 
     * @param job
     *            The job configuration.
     */
    public void configure(JobConf job) {
        // set the url filters, scoring filters the parse util and the url
        // normalizers
        this.jobConf = job;
        this.urlFilters = new URLFilters(jobConf);
        this.scfilters = new ScoringFilters(jobConf);
        this.parseUtil = new ParseUtil(jobConf);
        this.normalizers = new URLNormalizers(jobConf, URLNormalizers.SCOPE_FETCHER);
        this.interval = jobConf.getInt("db.fetch.interval.default", 2592000);

        this.httpStatusCodeFilter = new HTTPStatusCodeFilter(jobConf.get("nutchwax.filter.http.status"));

        this.seqFilePrefix = jobConf.get("nutchwax.importer.hdfs.seqfileprefix");
        this.seqFileSuffix = jobConf.get("nutchwax.importer.hdfs.seqfilesuffix");
        this.seqFilePath = jobConf.get("nutchwax.importer.hdfs.seqfilepath");
    }

    /**
     * In Mapper interface.
     * 
     * @inherit
     */
    public void close() {

    }

    /**
     * <p>
     * Runs the Map job to import records from an archive file into a Nutch
     * segment.
     * </p>
     * 
     * @param key
     *            Line number in manifest corresponding to the
     *            <code>value</code>
     * @param value
     *            A line from the manifest
     * @param output
     *            The output collecter.
     * @param reporter
     *            The progress reporter.
     */
    public void map(final WritableComparable key, final Writable value, final OutputCollector output,
            final Reporter reporter) throws IOException {
        String arcUrl = "";
        String collection = "";
        String segmentName = getConf().get(Nutch.SEGMENT_NAME_KEY);

        // First, ignore blank manifest lines, and those that are comments.
        String line = value.toString().trim();
        if (line.length() == 0 || line.charAt(0) == '#') {
            // Ignore it.
            return;
        }

        // Each line of the manifest is "<url> <collection>" where <collection>
        // is optional
        String[] parts = line.split("\\s+");
        arcUrl = parts[0];

        if (parts.length > 1) {
            collection = parts[1];
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Importing ARC: " + arcUrl);
        }

        ArchiveReader r = ArchiveReaderFactory.get(arcUrl);
        r.setDigest(true);

        ArcReader reader = new ArcReader(r);

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            // XXX : Rewritten to accomodate HDFS sequencefile writing
            String prefix = seqFilePrefix;
            String suffix = seqFileSuffix;
            String outfilepath = seqFilePath;
            String arcfilename = prefix + arcUrl.substring(arcUrl.lastIndexOf("/") + 1) + suffix;
            Path path = new Path(outfilepath + "/" + arcfilename);
            Writer writer = SequenceFile.createWriter(fs, conf, path, new Text().getClass(), new Text().getClass());

            for (ARCRecord record : reader) {
                // When reading WARC files, records of type other than
                // "response" are returned as 'null' by the Iterator, so
                // we skip them.
                if (record == null) {
                    continue;
                }

                importRecord(record, segmentName, collection, output, writer);

                reporter.progress();
            }
            writer.close();
            System.out.println("Finished processing " + arcfilename);

            /*
             * //test reading Reader seqreader = new Reader(fs, path, conf);
             * Text key_r = new Text(); Text val_r = new Text();
             * 
             * while (seqreader.next(key_r, val_r)) { System.out.println(key_r +
             * "\t len: " + val_r.getLength()); }
             */

        } catch (Exception e) {
            LOG.warn("Error processing archive file: " + arcUrl, e);

            if (jobConf.getBoolean("nutchwax.import.abortOnArchiveReadError", false)) {
                throw new IOException(e);
            }
        } finally {
            r.close();

            if (LOG.isInfoEnabled()) {
                LOG.info("Completed ARC: " + arcUrl);
            }
        }

    }

    /**
     * Import an ARCRecord.
     *
     * @param record
     * @param segmentName
     * @param collectionName
     * @param output
     * @return whether record was imported or not (i.e. filtered out due to URL
     *         filtering rules, etc.)
     */
    private boolean importRecord(ARCRecord record, String segmentName, String collectionName, OutputCollector output,
            Writer writer) {
        ARCRecordMetaData meta = record.getMetaData();

        if (LOG.isInfoEnabled()) {
            LOG.info("Consider URL: " + meta.getUrl() + " (" + meta.getMimetype() + ") [" + meta.getLength() + "]");
        }

        if (!this.httpStatusCodeFilter.isAllowed(record.getStatusCode())) {
            if (LOG.isInfoEnabled()) {
                LOG.info("Skip     URL: " + meta.getUrl() + " HTTP status:" + record.getStatusCode());
            }

            return false;
        }

        try {
            // Skip the HTTP headers in the response body, so that the
            // parsers are parsing the reponse body and not the HTTP
            // headers.
            record.skipHttpHeader();

            // We use record.available() rather than meta.getLength()
            // because the latter includes the size of the HTTP header,
            // which we just skipped.
            byte[] bytes = readBytes(record, record.available());

            // If there is no digest, then we assume we're reading an
            // ARCRecord not a WARCRecord. In that case, we close the
            // record, which updates the digest string. Then we tweak the
            // digest string so we have the same for for both ARC and WARC
            // records.
            if (meta.getDigest() == null) {
                record.close();

                // This is a bit hacky, but ARC and WARC records produce
                // two slightly different digest formats. WARC record
                // digests have the algorithm name as a prefix, such as
                // "sha1:PD3SS4WWZVFWTDC63RU2MWX7BVC2Y2VA" but the
                // ArcRecord.getDigestStr() does not. Since we want the
                // formats to match, we prepend the "sha1:" prefix to ARC
                // record digest.
                meta.setDigest("sha1:" + record.getDigestStr());
            }

            // Normalize and filter
            String url = this.normalizeAndFilterUrl(meta.getUrl(), meta.getDigest(), meta.getDate());

            if (url == null) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("Skip     URL: " + meta.getUrl());
                }
                return false;
            }

            // We create a key which combines the URL and digest values.
            // This is necessary because Nutch stores all the data in
            // MapFiles, which are basically just {key,value} pairs.
            //
            // If we use just the URL as the key (which is the way Nutch
            // usually works) then we have problems with multiple,
            // different copies of the same URL. If we try and store two
            // different copies of the same URL (each having a different
            // digest) and only use the URL as the key, when the MapFile
            // is written, only *one* copy of the page will be stored.
            //
            // Think about it, we're basically doing:
            // MapFile.put( url, value1 );
            // MapFile.put( url, value2 );
            // Only one of those url,value mappings will keep, the other
            // is over-written.
            //
            // So, by using the url+digest as the key, we can have all the
            // data stored. The only problem is all over in Nutch where
            // the key==url is assumed :(
            String key = url + " " + meta.getDigest();

            Metadata contentMetadata = new Metadata();
            // Set the segment name, just as is done by standard Nutch fetching.
            // Then, add the NutchWAX-specific metadata fields.
            contentMetadata.set(Nutch.SEGMENT_NAME_KEY, segmentName);

            // We store both the normal URL and the URL+digest key for
            // later retrieval by the indexing plugin(s).
            contentMetadata.set(NutchWax.URL_KEY, url);
            // contentMetadata.set( NutchWax.ORIG_KEY, key );

            contentMetadata.set(NutchWax.FILENAME_KEY, meta.getArcFile().getName());
            contentMetadata.set(NutchWax.FILEOFFSET_KEY, String.valueOf(record.getHeader().getOffset()));

            contentMetadata.set(NutchWax.COLLECTION_KEY, collectionName);
            contentMetadata.set(NutchWax.DATE_KEY, meta.getDate());
            contentMetadata.set(NutchWax.DIGEST_KEY, meta.getDigest());
            contentMetadata.set(NutchWax.CONTENT_TYPE_KEY, meta.getMimetype());
            contentMetadata.set(NutchWax.CONTENT_LENGTH_KEY, String.valueOf(meta.getLength()));
            contentMetadata.set(NutchWax.HTTP_RESPONSE_KEY, String.valueOf(record.getStatusCode()));

            Content content = new Content(url, url, bytes, meta.getMimetype(), contentMetadata, getConf());

            // -----------------
            // write to seqencefile

            byte[] contentInOctets = content.getContent();
            String htmlraw = new String();

            // meta only contains char encodings
            // LOG.info("Metadata count: " + contentMetadata.names().length);
            // for (String name : contentMetadata.names()){
            // LOG.info("meta " + name + " : " + contentMetadata.get(name));
            // }
            // try getting content encoding
            try {
                htmlraw = new String(contentInOctets, contentMetadata.get("OriginalCharEncoding"));
            } catch (Exception e) {
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

            URL url_h = null;
            try {
                url_h = new URL(content.getUrl());
            } catch (MalformedURLException e1) {
                LOG.error("Malformed URL Exception: " + e1.getMessage());
            }
            String hostname = url_h.getHost();
            String urlpath = url_h.getPath();
            // LOG.info("HOST:"+ hostname);
            // LOG.info("PATH:"+ urlpath);

            String date = meta.getDate();
            // LOG.info("meta date: " + date);
            Text key_h = new Text(hostname + "::" + urlpath + "::" + date);
            Text value = new Text(htmlraw);
            try {
                LOG.info("len: " + writer.getLength() + ", key: " + key_h + ", value len: " + value.getLength());
                writer.append(key_h, value);
            } catch (IOException e) {
                LOG.error("SequenceFile IOException: " + e.getMessage());
            }

            // -----------------

            output(output, new Text(key), content);

            return true;
        } catch (Throwable t) {
            LOG.error("Import fail : " + meta.getUrl(), t);
        }

        return false;
    }

    /**
     * Normalize and filter the URL. If the URL is malformed or filtered
     * (according to registered Nutch URL filtering plugins), return
     * <code>null</code>. Otherwise return the normalized URL.
     *
     * @param candidateUrl
     *            to be normalized and filtered
     * @param digest
     *            of URL content
     * @param date
     *            of URL capture
     * @return normalized URL, <code>null</code> if malformed or filtered out
     */
    private String normalizeAndFilterUrl(String candidateUrl, String digest, String date) {
        String url = null;
        try {
            url = normalizers.normalize(candidateUrl, URLNormalizers.SCOPE_FETCHER);

            if (urlFilters.filter(url + " " + digest + " " + date) != null) {
                return url;
            }
        } catch (MalformedURLException mue) {
            if (LOG.isInfoEnabled()) {
                LOG.info("MalformedURL: " + candidateUrl);
            }
        } catch (URLFilterException ufe) {
            if (LOG.isInfoEnabled()) {
                LOG.info("URL filtered: " + candidateUrl);
            }
        }

        return null;
    }

    /**
     * Writes the key and related content to the output collector. The division
     * between <code>importRecord</code> and <code>output</code> is merely based
     * on the way the code was structured in the
     * <code>ArcSegmentCreator.java</code> which was used as a starting-point
     * for this class.
     */
    private void output(OutputCollector output, Text key, Content content) {
        LOG.debug("output( " + key + " )");

        // Create the crawl datum. This
        CrawlDatum datum = new CrawlDatum(CrawlDatum.STATUS_FETCH_SUCCESS, this.interval, 1.0f);

        // ?: I have no idea why we need to store the ProtocolStatus in
        // the datum's metadata, but the Nutch Fetcher class does it and
        // it seems important. Since we're not really fetching here, we
        // assume ProtocolStatus.STATUS_SUCCESS is the right thing to do.
        datum.getMetaData().put(Nutch.WRITABLE_PROTO_STATUS_KEY, ProtocolStatus.STATUS_SUCCESS);

        // ?: Since we store the ARCRecord's archival date in the Content
        // object, we follow the
        // logic in Nutch's Fetcher and store the current import time/date in
        // the Datum. I have
        // no idea if it makes a difference, other than this value is stored in
        // the "tstamp"
        // field in the Lucene index whereas the ARCRecord date is stored in the
        // "date" field
        // we added above.
        datum.setFetchTime(System.currentTimeMillis());

        // ?: It doesn't seem to me that we need/use the scoring stuff
        // one way or another, but we might as well leave it in.
        try {
            scfilters.passScoreBeforeParsing(key, datum, content);
        } catch (Exception e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Couldn't pass score before parsing for: " + key, e);
            }
        }

        // ?: This is kind of interesting. In the Nutch Fetcher class, if the
        // parsing fails,
        // the Content is not added to the output. But in Importer, we still add
        // it, even
        // if the parsing fails. Why?
        //
        // One benefit is that even if the parsing fails, having the Content in
        // the index still
        // allows us to find the document by URL, date, etc.
        //
        // However, I don't know what will happen when a summary is
        // computed...if the Content isn't there, will
        // it fail or just return an empty summary?
        ParseResult parseResult = null;
        try {
            parseResult = this.parseUtil.parse(content);
        } catch (Exception e) {
            LOG.warn("Error parsing: " + key, e);
        }

        // ?: This is taken from Nutch Fetcher. I believe the signatures are
        // used in the Fetcher
        // to ensure that URL contents are not stored multiple times if the
        // signature doesn't change.
        // Makes sense. But, in our case, we're relying on the (W)ARC production
        // tools to eliminate
        // duplicate data (or are we?), so how important is the signature for
        // our purposes?
        // I'll go ahead and leave it in, in case it's needed by Nutch for
        // unknown purposes.
        //
        // Also, since we still import documents even if the parsing fails, we
        // compute a signature
        // using an "empty" Parse object in the case of parse failure. I don't
        // know why we create
        // an empty Parse object rather than just use 'null', but I'm copying
        // the way the Fetcher
        // does it.
        //
        // One odd thing is that we add the signature to the datum here, then
        // "collect" the datum
        // just below, but then after collecting the datum, we update the
        // signature when processing
        // the ParseResults. I guess "collecting" doesn't write out the datum,
        // but "collects" it for
        // later output, thus we can update it after collection (I guess).
        if (parseResult == null) {
            byte[] signature = SignatureFactory.getSignature(getConf()).calculate(content,
                    new ParseStatus().getEmptyParse(getConf()));
            datum.setSignature(signature);
        }

        try {
            // Some weird problem with Hadoop 0.19.x - when the crawl_data
            // is merged during the reduce step, the classloader cannot
            // find the org.apache.nutch.protocol.ProtocolStatus class.
            //
            // We avoid the whole issue by omitting the crawl_data all
            // together, which we don't use anyways.
            //
            // output.collect( key, new NutchWritable( datum ) );

            if (jobConf.getBoolean("nutchwax.import.store.content", false)) {
                output.collect(key, new NutchWritable(content));
            }

            if (parseResult != null) {
                for (Entry<Text, Parse> entry : parseResult) {
                    Text url = entry.getKey();
                    Parse parse = entry.getValue();
                    ParseStatus parseStatus = parse.getData().getStatus();

                    if (!parseStatus.isSuccess()) {
                        LOG.warn("Error parsing: " + key + ": " + parseStatus);
                        parse = parseStatus.getEmptyParse(getConf());
                    }

                    byte[] signature = SignatureFactory.getSignature(getConf()).calculate(content, parse);

                    // ?: Why bother setting this one again? According to
                    // ParseData Javadoc,
                    // the getContentMeta() returns the original Content
                    // metadata object, so
                    // why are we setting the segment name on it to the same
                    // value again?
                    // Let's leave it out.
                    // parse.getData().getContentMeta().set(
                    // Nutch.SEGMENT_NAME_KEY, segmentName );

                    // ?: These two are copied from Nutch's Fetcher
                    // implementation.
                    parse.getData().getContentMeta().set(Nutch.SIGNATURE_KEY, StringUtil.toHexString(signature));
                    parse.getData().getContentMeta().set(Nutch.FETCH_TIME_KEY, Long.toString(datum.getFetchTime()));

                    // ?: What is this all about? It was in the original
                    // ArcSegmentCreator.java that
                    // inspired this code. But I can't figure out why we need
                    // it. If anything
                    // this will always be false since our key is now
                    // URL+digest, not just URL.
                    // Since it's always false, let's leave it out.
                    /*
                     * if ( url.equals( key ) ) { datum.setSignature( signature
                     * ); } else { if ( LOG.isWarnEnabled() ) LOG.warn(
                     * "ParseResult entry key and url differ: key=" + key +
                     * " url=" + url ); }
                     */

                    // ?: As above, we'll leave the scoring hooks in place.
                    try {
                        scfilters.passScoreAfterParsing(url, content, parse);
                    } catch (Exception e) {
                        if (LOG.isWarnEnabled()) {
                            LOG.warn("Couldn't pass score, url = " + key, e);
                        }
                    }

                    output.collect(key, new NutchWritable(new ParseImpl(new ParseText(parse.getText()),
                            parse.getData(), parse.isCanonical())));
                }
            }
        } catch (Exception e) {
            LOG.error("Error outputting Nutch record for: " + key, e);
        }
    }

    /**
     * Utility method to read the content bytes from an archive record. The
     * number of bytes read can be limited via the configuration property
     * <code>nutchwax.import.content.limit</code>.
     */
    private byte[] readBytes(ARCRecord record, long contentLength) throws IOException {
        // Ensure the record does strict reading.
        record.setStrict(true);

        long size = jobConf.getLong("nutchwax.import.content.limit", -1);

        if (size < 0) {
            size = contentLength;
        } else {
            size = Math.min(size, contentLength);
        }

        // Read the bytes of the HTTP response
        byte[] bytes = new byte[(int) size];

        if (size == 0) {
            return bytes;
        }

        // NOTE: Do not use read(byte[]) because ArchiveRecord does NOT
        // over-ride
        // the implementation inherited from InputStream. And since it does
        // not over-ride it, it won't do the digesting on it. Must use either
        // read(byte[],offset,length) or read().
        int pos = 0;
        while ((pos += record.read(bytes, pos, (bytes.length - pos))) < bytes.length)
            ;

        // Now that the bytes[] buffer has been filled, read the remainder
        // of the record so that the digest is computed over the entire
        // content.
        byte[] buf = new byte[1024 * 1024];
        int count = 0;
        while (record.available() > 0) {
            count += record.read(buf, 0, Math.min(buf.length, record.available()));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Bytes read: expected=" + contentLength + " bytes.length=" + bytes.length + " pos=" + pos
                    + " count=" + count);
        }

        // Sanity check. The number of bytes read into our bytes[]
        // buffer, plus the count of extra stuff read after it should
        // equal the contentLength passed into this function.
        if (pos + count != contentLength) {
            throw new IOException("Incorrect number of bytes read from ArchiveRecord: expected=" + contentLength
                    + " bytes.length=" + bytes.length + " pos=" + pos + " count=" + count);
        }

        return bytes;
    }

    /**
     * Runs the import job with the given arguments. This method assumes that is
     * is being run via the command-line; as such, it emits error messages
     * regarding invalid/missing arguments to the system error stream.
     */
    public int run(String[] args) throws Exception {
        if (args.length < 1) {
            usage();
            return -1;
        }

        JobConf job = new NutchJob(getConf());

        // Check for "-e <exclusions>" option.
        int pos = 0;
        if (args[0].equals("-e")) {
            if (args.length < 2) {
                System.out.println("ERROR: Missing filename for option \"-e\"\n");
                usage();
                return -1;
            }

            job.set("nutchwax.urlfilter.wayback.exclusions", args[1]);

            pos = 2;
        }

        if (args.length - pos < 1) {
            System.out.println("ERROR: Missing manifest file.\n");
            usage();
            return -1;
        }

        Path manifestPath = new Path(args[pos++]);

        Path segmentPath;
        if (args.length - pos < 1) {
            segmentPath = new Path("segments", org.apache.nutch.crawl.Generator.generateSegmentName());
        } else {
            segmentPath = new Path(args[pos]);
        }

        try {
            job.setJobName("Importer_to_Hdfs " + manifestPath);
            job.set(Nutch.SEGMENT_NAME_KEY, segmentPath.getName());

            // job.setInputPath ( manifestPath);
            FileInputFormat.addInputPath(job, manifestPath);
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(ImporterToHdfs.class);

            // job.setOutputPath ( segmentPath );
            FileOutputFormat.setOutputPath(job, segmentPath);
            job.setOutputFormat(FetcherOutputFormat.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NutchWritable.class);

            RunningJob rj = JobClient.runJob(job);

            return rj.isSuccessful() ? 0 : 1;
        } catch (Exception e) {
            LOG.fatal("Importer_to_Hdfs: ", e);
            System.out.println("Fatal error: " + e);
            e.printStackTrace(System.out);
            return -1;
        }
    }

    /**
     * Emit usage information for command-line driver.
     */
    public void usage() {
        String usage = "Usage: import_to_hdfs [opts] <manifest> [<segment>]\n" + "Options:\n"
                + "  -e filename     Exclusions file, over-rides configuration property.\n" + "\n"
                + "If <segment> not specified, a pathname will be automatically generated\n"
                + "based on current time in sub-directory 'segments', which is created if\n"
                + "necessary.  This is to mirror the behavior of other Nutch actions.\n";

        System.out.println(usage);
    }

    /**
     * Command-line driver. Runs the Importer as a Hadoop job.
     */
    public static void main(String args[]) throws Exception {
        int result = ToolRunner.run(NutchConfiguration.create(), new ImporterToHdfs(), args);

        System.exit(result);
    }

}

/**
 * This should all be moved into some sort of filtering plugin. Unfortunately
 * the URLFilter plugin interface isn't adequate as it only looks at a URL
 * string. Rather than jamming a response code through that interface, we do a
 * one-off filter class here.
 *
 * A long-term solution would be to create a new Nutch extension point interface
 * that takes an ARCRecord rather than a URL string. That way we can write
 * filters that can operate on any part of an ARCRecord, not just the URL.
 */
class HTTPStatusCodeFilter {
    List<Range> ranges = new ArrayList<Range>();

    public HTTPStatusCodeFilter(String configuration) {
        if (configuration == null) {
            return;
        }

        configuration = configuration.trim();

        for (String value : configuration.split("\\s+")) {
            Range range = new Range();

            // Special handling for "unknown" where an ARCRecord doesn't have
            // an HTTP status code. The ARCRecord.getStatusCode() returns
            // -1 in that case, so we make a range for it.
            if (value.toLowerCase().equals("unknown")) {
                range.lower = -1;
                range.upper = -1;

                this.ranges.add(range);

                continue;
            }

            String values[] = value.split("[-]");

            try {
                switch (values.length) {
                case 2:
                    // It's a range, N-M
                    range.lower = Integer.parseInt(values[0]);
                    range.upper = Integer.parseInt(values[1]);
                    break;

                case 1:
                    // It's a single value, convert to a single-value range
                    range.lower = Integer.parseInt(values[0]);
                    range.upper = range.lower;
                    break;

                default:
                    // Bad format
                    ImporterToHdfs.LOG.warn("Illegal format for nutchwax.filter.http.status: " + range);
                    continue;
                }

                this.ranges.add(range);
            } catch (NumberFormatException nfe) {
                ImporterToHdfs.LOG.warn("Illegal format for nutchwax.filter.http.status: " + range, nfe);
            }
        }

    }

    public boolean isAllowed(int code) {
        for (Range r : this.ranges) {
            return (r.lower <= code && code <= r.upper);
        }

        return false;
    }

    static class Range {
        int lower;
        int upper;
    }
}
