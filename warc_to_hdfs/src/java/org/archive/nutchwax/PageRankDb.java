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

import java.io.*;
import java.util.*;
import java.net.*;

// Commons Logging imports
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.nutch.crawl.LinkDbFilter;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LockUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * <p>Maintains an inverted link map, listing incoming links for each
 * url.</p>
 * <p>Aaron Binns @ archive.org: see comments in PageRankDbMerger.</p>
*/
public class PageRankDb extends Configured 
  implements Tool, Mapper<Text, ParseData, Text, IntWritable>
{
  public static final Log LOG = LogFactory.getLog(PageRankDb.class);
  
  public static final String CURRENT_NAME = "current";
  public static final String LOCK_NAME = ".locked";
  
  private int maxAnchorLength;
  private boolean ignoreInternalLinks;
  private URLFilters urlFilters;
  private URLNormalizers urlNormalizers;
  
  public PageRankDb( ) 
  {
  }
  
  public PageRankDb( Configuration conf )
  {
    setConf(conf);
  }
  
  public void configure( JobConf job )
  {
    ignoreInternalLinks = job.getBoolean("db.ignore.internal.links", true);
    if (job.getBoolean(LinkDbFilter.URL_FILTERING, false)) 
      {
        urlFilters = new URLFilters(job);
      }
    if (job.getBoolean(LinkDbFilter.URL_NORMALIZING, false)) 
      {
        urlNormalizers = new URLNormalizers(job, URLNormalizers.SCOPE_LINKDB);
      }
  }

  public void close( ) 
  {
  }
  
  public void map( Text key, ParseData parseData, OutputCollector<Text, IntWritable> output, Reporter reporter )
    throws IOException 
  {
    String fromUrl  = key.toString();
    String fromHost = getHost(fromUrl);

    if (urlNormalizers != null) 
      {
        try 
          {
            fromUrl = urlNormalizers.normalize(fromUrl, URLNormalizers.SCOPE_LINKDB); // normalize the url
          }
        catch (Exception e) 
          {
            LOG.warn("Skipping " + fromUrl + ":" + e);
            fromUrl = null;
          }
      }
    if (fromUrl != null && urlFilters != null) 
      {
        try 
          {
            fromUrl = urlFilters.filter(fromUrl); // filter the url
          }
        catch (Exception e) 
          {
            LOG.warn("Skipping " + fromUrl + ":" + e);
            fromUrl = null;
          }
      }
    if (fromUrl == null) return;

    Outlink[] outlinks = parseData.getOutlinks();

    for (int i = 0; i < outlinks.length; i++) 
      {
        Outlink outlink = outlinks[i];
        String toUrl = outlink.getToUrl();

        if (ignoreInternalLinks) 
          {
            String toHost = getHost(toUrl);
            if (toHost == null || toHost.equals(fromHost)) 
              { // internal link
                continue;                               // skip it
              }
          }
        if (urlNormalizers != null) 
          {
            try 
              {
                toUrl = urlNormalizers.normalize(toUrl, URLNormalizers.SCOPE_LINKDB); // normalize the url
              }
            catch (Exception e) 
              {
                LOG.warn("Skipping " + toUrl + ":" + e);
                toUrl = null;
              }
          }
        if (toUrl != null && urlFilters != null) 
          {
            try 
              {
                toUrl = urlFilters.filter(toUrl); // filter the url
              }
            catch (Exception e) 
              {
                LOG.warn("Skipping " + toUrl + ":" + e);
                toUrl = null;
              }
          }

        if (toUrl == null) continue;

        // DIFF: We just emit a count of '1' for the toUrl.  That's it.
        //       Rather than the list of inlinks as in LinkDb.
        output.collect( new Text(toUrl), new IntWritable( 1 ) );
      }
  }

  private String getHost(String url) 
  {
    try 
      {
        return new URL(url).getHost().toLowerCase();
      }
    catch (MalformedURLException e) 
      {
        return null;
      }
  }

  public void invert(Path pageRankDb, final Path segmentsDir, boolean normalize, boolean filter, boolean force) throws IOException 
  {
    final FileSystem fs = FileSystem.get(getConf());
    FileStatus[] files = fs.listStatus(segmentsDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
    invert(pageRankDb, HadoopFSUtil.getPaths(files), normalize, filter, force);
  }

  public void invert(Path pageRankDb, Path[] segments, boolean normalize, boolean filter, boolean force) throws IOException 
  {

    Path lock = new Path(pageRankDb, LOCK_NAME);
    FileSystem fs = FileSystem.get(getConf());
    LockUtil.createLockFile(fs, lock, force);
    Path currentPageRankDb = new Path(pageRankDb, CURRENT_NAME);
    if (LOG.isInfoEnabled()) 
      {
        LOG.info("PageRankDb: starting");
        LOG.info("PageRankDb: pageRankDb: " + pageRankDb);
        LOG.info("PageRankDb: URL normalize: " + normalize);
        LOG.info("PageRankDb: URL filter: " + filter);
      }
    JobConf job = PageRankDb.createJob(getConf(), pageRankDb, normalize, filter);
    for (int i = 0; i < segments.length; i++) 
      {
        if (LOG.isInfoEnabled()) 
          {
            LOG.info("PageRankDb: adding segment: " + segments[i]);
          }
        FileInputFormat.addInputPath(job, new Path(segments[i], ParseData.DIR_NAME));
      }
    try 
      {
        JobClient.runJob(job);
      }
    catch (IOException e) 
      {
        LockUtil.removeLockFile(fs, lock);
        throw e;
      }
    if (fs.exists(currentPageRankDb)) 
      {
        if (LOG.isInfoEnabled()) 
          {
            LOG.info("PageRankDb: merging with existing pageRankDb: " + pageRankDb);
          }
        // try to merge
        Path newPageRankDb = FileOutputFormat.getOutputPath(job);
        job = PageRankDbMerger.createMergeJob(getConf(), pageRankDb, normalize, filter);
        FileInputFormat.addInputPath(job, currentPageRankDb);
        FileInputFormat.addInputPath(job, newPageRankDb);
        try 
          {
            JobClient.runJob(job);
          }
        catch (IOException e) 
          {
            LockUtil.removeLockFile(fs, lock);
            fs.delete(newPageRankDb, true);
            throw e;
          }
        fs.delete(newPageRankDb, true);
      }
    PageRankDb.install(job, pageRankDb);
    if (LOG.isInfoEnabled()) 
      { LOG.info("PageRankDb: done"); }
  }

  private static JobConf createJob(Configuration config, Path pageRankDb, boolean normalize, boolean filter) 
  {
    Path newPageRankDb = new Path("pagerankdb-" + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("pagerankdb " + pageRankDb);

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(PageRankDb.class);
    job.setCombinerClass(PageRankDbMerger.class);
    // if we don't run the mergeJob, perform normalization/filtering now
    if (normalize || filter) 
      {
        try 
          {
            FileSystem fs = FileSystem.get(config);
            if (!fs.exists(pageRankDb)) 
              {
                job.setBoolean(LinkDbFilter.URL_FILTERING, filter);
                job.setBoolean(LinkDbFilter.URL_NORMALIZING, normalize);
              }
          }
        catch (Exception e) 
          {
            LOG.warn("PageRankDb createJob: " + e);
          }
      }
    job.setReducerClass(PageRankDbMerger.class);

    FileOutputFormat.setOutputPath(job, newPageRankDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", false);
    job.setOutputKeyClass(Text.class);

    // DIFF: Use IntWritable instead of Inlinks as the output value type.
    job.setOutputValueClass(IntWritable.class);

    return job;
  }

  public static void install(JobConf job, Path pageRankDb) throws IOException 
  {
    Path newPageRankDb = FileOutputFormat.getOutputPath(job);
    FileSystem fs = new JobClient(job).getFs();
    Path old = new Path(pageRankDb, "old");
    Path current = new Path(pageRankDb, CURRENT_NAME);
    if (fs.exists(current)) 
      {
        if (fs.exists(old)) fs.delete(old, true);
        fs.rename(current, old);
      }
    fs.mkdirs(pageRankDb);
    fs.rename(newPageRankDb, current);
    if (fs.exists(old)) fs.delete(old, true);
    LockUtil.removeLockFile(fs, new Path(pageRankDb, LOCK_NAME));
  }

  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(NutchConfiguration.create(), new PageRankDb(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception 
  {
    if (args.length < 2) 
      {
        System.err.println("Usage: PageRankDb <pagerankdb> (-dir <segmentsDir> | <seg1> <seg2> ...) [-force] [-noNormalize] [-noFilter]");
        System.err.println("\tpagerankdb\toutput PageRankDb to create or update");
        System.err.println("\t-dir segmentsDir\tparent directory of several segments, OR");
        System.err.println("\tseg1 seg2 ...\t list of segment directories");
        System.err.println("\t-force\tforce update even if PageRankDb appears to be locked (CAUTION advised)");
        System.err.println("\t-noNormalize\tdon't normalize link URLs");
        System.err.println("\t-noFilter\tdon't apply URLFilters to link URLs");
        return -1;
      }
    Path segDir = null;
    final FileSystem fs = FileSystem.get(getConf());
    Path db = new Path(args[0]);
    ArrayList<Path> segs = new ArrayList<Path>();
    boolean filter = true;
    boolean normalize = true;
    boolean force = false;
    for (int i = 1; i < args.length; i++) 
      {
        if (args[i].equals("-dir")) 
          {
            segDir = new Path(args[++i]);
            FileStatus[] files = fs.listStatus(segDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
            if (files != null) segs.addAll(Arrays.asList(HadoopFSUtil.getPaths(files)));
            break;
          }
        else if (args[i].equalsIgnoreCase("-noNormalize")) 
          {
            normalize = false;
          }
        else if (args[i].equalsIgnoreCase("-noFilter")) 
          {
            filter = false;
          }
        else if (args[i].equalsIgnoreCase("-force")) 
          {
            force = true;
          }
        else segs.add(new Path(args[i]));
      }
    try 
      {
        invert(db, segs.toArray(new Path[segs.size()]), normalize, filter, force);
        return 0;
      }
    catch (Exception e) 
      {
        LOG.fatal("PageRankDb: " + StringUtils.stringifyException(e));
        return -1;
      }
  }

}
