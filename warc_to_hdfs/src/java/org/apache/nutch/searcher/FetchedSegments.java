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

package org.apache.nutch.searcher;

import java.io.IOException;
import java.io.Reader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.nutch.protocol.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.nutch.crawl.*;

/** Implements {@link HitSummarizer} and {@link HitContent} for a set of
 * fetched segments. */
public class FetchedSegments implements RPCSegmentBean {

  public static final Log LOG = LogFactory.getLog(FetchedSegments.class);

  public static final long VERSION = 1L;

  private static final ExecutorService executor =
    Executors.newCachedThreadPool();

  private class SummaryTask implements Callable<Summary> {
    private final HitDetails details;
    private final Query query;

    public SummaryTask(HitDetails details, Query query) {
      this.details = details;
      this.query = query;
    }

    public Summary call() throws Exception {
      return getSummary(details, query);
    }
  }

  /*
  private class SegmentUpdater extends Thread {

    @Override
    public void run() {
      while (true) {
        try {
          final FileStatus[] fstats = fs.listStatus(segmentsDir,
              HadoopFSUtil.getPassDirectoriesFilter(fs));
          final Path[] segmentDirs = HadoopFSUtil.getPaths(fstats);
          final Iterator<Map.Entry<String, Segment>> i = segments.entrySet().iterator();
          while (i.hasNext()) {
            final Map.Entry<String, Segment> entry = i.next();
            final Segment seg = entry.getValue();
            if (!fs.exists(seg.segmentDir)) {
              try {
                seg.close();
              } catch (final Exception e) {
                / * A segment may fail to close
                 * since it may already be deleted from
                 * file system. So we just ignore the
                 * exception and remove the mapping from
                 * 'segments'.
                 * /
              } finally {
                i.remove();
              }
            }
          }

          if (segmentDirs != null) {
            for (final Path segmentDir : segmentDirs) {
              segments.putIfAbsent(segmentDir.getName(),
                  new Segment(fs, segmentDir, conf));
            }
          }

          Thread.sleep(60000);
        } catch (final InterruptedException e) {
          // ignore
        } catch (final IOException e) {
          // ignore
        }
      }
    }

  }
  */

  private static class Segment implements java.io.Closeable {

    private static final Partitioner<Text, Writable> PARTITIONER =
      new HashPartitioner<Text, Writable>();

    private final FileSystem fs;
    private final Path segmentDir;

    private MapFile.Reader[] content;
    private MapFile.Reader[] parseText;
    private MapFile.Reader[] parseData;
    private MapFile.Reader[] crawl;
    private final Configuration conf;

    public Segment(FileSystem fs, Path segmentDir, Configuration conf) throws IOException {
      this.fs = fs;
      this.segmentDir = segmentDir;
      this.conf = conf;
    }

    public CrawlDatum getCrawlDatum(Text url) throws IOException {
      synchronized (this) {
        if (crawl == null)
          crawl = getReaders(CrawlDatum.FETCH_DIR_NAME);
      }
      return (CrawlDatum)getEntry(crawl, url, new CrawlDatum());
    }

    public byte[] getContent(Text url) throws IOException {
      synchronized (this) {
        if (content == null)
          content = getReaders(Content.DIR_NAME);
      }
      return ((Content)getEntry(content, url, new Content())).getContent();
    }

    public ParseData getParseData(Text url) throws IOException {
      synchronized (this) {
        if (parseData == null)
          parseData = getReaders(ParseData.DIR_NAME);
      }
      return (ParseData)getEntry(parseData, url, new ParseData());
    }

    public ParseText getParseText(Text url) throws IOException {
      synchronized (this) {
        if (parseText == null)
          parseText = getReaders(ParseText.DIR_NAME);
      }
      return (ParseText)getEntry(parseText, url, new ParseText());
    }

    private MapFile.Reader[] getReaders(String subDir) throws IOException {
      return MapFileOutputFormat.getReaders(fs, new Path(segmentDir, subDir), this.conf);
    }

    private Writable getEntry(MapFile.Reader[] readers, Text url,
                              Writable entry) throws IOException {
      return MapFileOutputFormat.getEntry(readers, PARTITIONER, url, entry);
    }

    public void close() throws IOException {
      if (content != null) { closeReaders(content); }
      if (parseText != null) { closeReaders(parseText); }
      if (parseData != null) { closeReaders(parseData); }
      if (crawl != null) { closeReaders(crawl); }
    }

    private void closeReaders(MapFile.Reader[] readers) throws IOException {
      for (int i = 0; i < readers.length; i++) {
        readers[i].close();
      }
    }

  }

  //private final ConcurrentMap<String, Segment> segments = new ConcurrentHashMap<String, Segment>();
  private final ConcurrentMap segments = new ConcurrentHashMap();
  private       boolean perCollection = false;
  private final FileSystem fs;
  private final Configuration conf;
  private final Path segmentsDir;
  //private final SegmentUpdater segUpdater;
  private final Summarizer summarizer;

  /** Construct given a directory containing fetcher output. */
  public FetchedSegments(Configuration conf, Path segmentsDir)
  throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(this.conf);
    final FileStatus[] fstats = fs.listStatus(segmentsDir,
        HadoopFSUtil.getPassDirectoriesFilter(fs));
    final Path[] segmentDirs = HadoopFSUtil.getPaths(fstats);
    this.summarizer = new SummarizerFactory(this.conf).getSummarizer();
    this.segmentsDir = segmentsDir;
    //this.segUpdater = new SegmentUpdater();

    if ( segmentDirs == null )
      {
        LOG.warn( "No segment directories: " + segmentsDir );
        return ;
      }

    this.perCollection = conf.getBoolean( "nutchwax.FetchedSegments.perCollection", false );

    LOG.info( "Per-collection segments: " + this.perCollection );

    for ( int i = 0; i < segmentDirs.length; i++ )
      {
        if ( this.perCollection )
          {
            // Assume segmentDir is actually a 'collection' dir which
            // contains a list of segments, such as:
            //   crawl/segments/194/segment-foo
            //                     /segment-bar
            //                     /segment-baz
            //   crawl/segments/366/segment-frotz
            //                     /segment-fizzle
            //                     /segment-bizzle
            // The '194' and '366' are collection dirs, which contain the
            // actual segment dirs.
            Path collectionDir = segmentDirs[i];
            
            Map perCollectionSegments = (Map) this.segments.get( collectionDir.getName( ) );
            if ( perCollectionSegments == null )
              {
                perCollectionSegments = new ConcurrentHashMap( );
                this.segments.put( collectionDir.getName( ), perCollectionSegments );
              }
            
            // Now, get a list of all the sub-dirs of the collectionDir,
            // and create segments for them, adding them to the
            // per-collection map.
            Path[] perCollectionSegmentDirs = HadoopFSUtil.getPaths( fs.listStatus( collectionDir, HadoopFSUtil.getPassDirectoriesFilter(fs) ) );
            for ( Path segmentDir : perCollectionSegmentDirs )
              {
                perCollectionSegments.put( segmentDir.getName( ), new Segment( fs, segmentDir, conf ) );
              }

            addRemaps( fs, collectionDir, (Map<String,Segment>) perCollectionSegments );
          }
        else
          {
            Path segmentDir = segmentDirs[i];
            segments.put(segmentDir.getName(), new Segment(this.fs, segmentDir, this.conf));
          }
      }
    
    // this.segUpdater.start();
  }

  protected void addRemaps( FileSystem fs, Path segmentDir, Map<String,Segment> segments )
    throws IOException
  {
    Path segmentRemapFile = new Path( segmentDir, "remap" );

    if ( ! fs.exists( segmentRemapFile ) )
      {
        LOG.warn( "Remap file doesn't exist: " + segmentRemapFile );
        
        return ;
      }

    // InputStream is = segmentRemapFile.getFileSystem( conf ).open( segmentRemapFile );
    InputStream is = fs.open( segmentRemapFile );
    
    BufferedReader reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
            
    String line;
    while ( (line = reader.readLine()) != null )
      {
        String fields[] = line.trim( ).split( "\\s+" );
        
        if ( fields.length < 2 )
          {
            LOG.warn( "Malformed remap line, not enough fields ("+fields.length+"): " + line );
            continue ;
          }
        
        // Look for the "to" name in the segments.
        Segment toSegment = segments.get( fields[1] );
        if ( toSegment == null )
          {
            LOG.warn( "Segment remap destination doesn't exist: " + fields[1] );
          }
        else
          {
            LOG.warn( "Remap: " + fields[0] + " => " + fields[1] );
            segments.put( fields[0], toSegment );
          }
      }
  }

  public String[] getSegmentNames() {
    return (String[])segments.keySet().toArray(new String[segments.size()]);
  }

  public byte[] getContent(HitDetails details) throws IOException {
    return getSegment(details).getContent(getKey(details));
  }

  public ParseData getParseData(HitDetails details) throws IOException {
    return getSegment(details).getParseData(getKey(details));
  }

  public long getFetchDate(HitDetails details) throws IOException {
    return getSegment(details).getCrawlDatum(getKey(details))
      .getFetchTime();
  }

  public ParseText getParseText(HitDetails details) throws IOException {
    return getSegment(details).getParseText(getKey(details));
  }

  public Summary getSummary(HitDetails details, Query query)
    throws IOException {

    if (this.summarizer == null) { return new Summary(); }

    String text = "";
    Segment segment = getSegment(details);

    if ( segment != null )
      {
        try
          {
            ParseText parseText = segment.getParseText(getKey(details));
            text = (parseText != null ) ? parseText.getText() : "";
          }
        catch ( Exception e )
          {
            LOG.error( "segment = " + segment.segmentDir, e );
          }
      }
    else
      {
        LOG.warn( "No segment for: " + details );
      }

    return this.summarizer.getSummary(text, query);
  }

  public long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return VERSION;
  }

  public Summary[] getSummary(HitDetails[] details, Query query)
    throws IOException {
    final List<Callable<Summary>> tasks =
      new ArrayList<Callable<Summary>>(details.length);
    for (int i = 0; i < details.length; i++) {
      tasks.add(new SummaryTask(details[i], query));
    }

    List<Future<Summary>> summaries;
    try {
      summaries = executor.invokeAll(tasks);
    } catch (final InterruptedException e) {
      throw new RuntimeException(e);
    }


    final Summary[] results = new Summary[details.length];
    for (int i = 0; i < details.length; i++) {
      final Future<Summary> f = summaries.get(i);
      Summary summary;
      try {
        summary = f.get();
      } catch (final Exception e) {
        if (e.getCause() instanceof IOException) {
          throw (IOException) e.getCause();
        }
        throw new RuntimeException(e);
      }
      results[i] = summary;
    }
    return results;
  }


  private Segment getSegment(HitDetails details) 
  {
    if ( this.perCollection )
      {
        LOG.info( "getSegment: " + details );
        LOG.info( "  collection: " + details.getValue("collection") );
        LOG.info( "  segment   : " + details.getValue("segment") );

        String collectionId = details.getValue("collection");
        String segmentName  = details.getValue("segment");
        
        Map perCollectionSegments = (Map) this.segments.get( collectionId );

        if ( perCollectionSegments == null )
          {
            LOG.warn( "Cannot find per-collection segments for: " + collectionId );

            return null;
          }
        
        Segment segment = (Segment) perCollectionSegments.get( segmentName );
        
        if ( segment == null )
          {
            LOG.warn( "Cannot find segment: collection=" + collectionId + " segment=" + segmentName );
          }

        return segment;
      }
    else
      {
        LOG.info( "getSegment: " + details );
        LOG.info( "  segment   : " + details.getValue("segment") );

        String segmentName = details.getValue( "segment" );
        Segment segment = (Segment) segments.get( segmentName );

        if ( segment == null )
          {
            LOG.warn( "Cannot find segment: " + segmentName );
          }
        
        return segment;
      }
  }

  private Text getKey(HitDetails details) 
  {
    String url = details.getValue("url") + " " + details.getValue("digest");
    
    return new Text(url);
  }

  public void close() throws IOException {
    final Iterator<Segment> iterator = segments.values().iterator();
    while (iterator.hasNext()) {
      iterator.next().close();
    }
  }

}
