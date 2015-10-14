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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.crawl.LinkDbFilter;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

/**
 * This tool merges several PageRankDb-s into one, optionally filtering
 * URLs through the current URLFilters, to skip prohibited URLs and
 * links.
 * 
 * <p>It's possible to use this tool just for filtering - in that case
 * only one PageRankDb should be specified in arguments.</p>
 * <p>If more than one PageRankDb contains information about the same URL,
 * all inlinks are accumulated, but only at most <code>db.max.inlinks</code>
 * inlinks will ever be added.</p>
 * <p>If activated, URLFilters will be applied to both the target URLs and
 * to any incoming link URL. If a target URL is prohibited, all
 * inlinks to that target will be removed, including the target URL. If
 * some of incoming links are prohibited, only they will be removed, and they
 * won't count when checking the above-mentioned maximum limit.</p>
 * <p>Aaron Binns @ archive.org:
 * <blockquote>
 *   Copy/paste/edit from LinkDbMerger.  We only care about the inlink
 *   <em>count</em> not the inlinks themsevles.  In fact, trying to
 *   retain the inlinks doesn't scale when processing 100s of millions
 *   of documents.  In large part, due to fact that that Inlinks
 *   object wants to keep all of the inlinks in memory at once,
 *   i.e. in a Set.  This doesn't work when we have 600 million
 *   documents and a single URL could easily have a million inlinks.
 * </blockquote></p>
 *
 * @author Andrzej Bialecki
 * @author Aaron Binns (archive.org)
 */
public class PageRankDbMerger extends Configured 
  implements Tool, Reducer<Text, IntWritable, Text, IntWritable> 
{
  private static final Log LOG = LogFactory.getLog(PageRankDbMerger.class);
  
  private int maxInlinks;
  
  public PageRankDbMerger() 
  {
    
  }
  
  public PageRankDbMerger(Configuration conf) 
  {
    setConf(conf);
  }

  public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 
  {
    // DIFF: Simply sum the count values for the key. 
    int count = 0;
    while ( values.hasNext( ) )
      {
        count += values.next( ).get( );
      }
    output.collect( key, new IntWritable( count ) );
  }

  public void configure(JobConf job) 
  {
    maxInlinks = job.getInt("db.max.inlinks", 10000);
  }

  public void close() throws IOException 
  { }

  public void merge(Path output, Path[] dbs, boolean normalize, boolean filter) throws Exception 
  {
    JobConf job = createMergeJob(getConf(), output, normalize, filter);
    for (int i = 0; i < dbs.length; i++) 
      {
        FileInputFormat.addInputPath(job, new Path(dbs[i], PageRankDb.CURRENT_NAME));      
      }
    JobClient.runJob(job);
    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(output);
    fs.rename(FileOutputFormat.getOutputPath(job), new Path(output, PageRankDb.CURRENT_NAME));
  }

  public static JobConf createMergeJob(Configuration config, Path pageRankDb, boolean normalize, boolean filter) 
  {
    Path newPageRankDb =
      new Path("pagerankdb-merge-" + 
               Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    JobConf job = new NutchJob(config);
    job.setJobName("pagerankdb merge " + pageRankDb);

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(PageRankDbFilter.class);
    job.setBoolean(LinkDbFilter.URL_NORMALIZING, normalize);
    job.setBoolean(LinkDbFilter.URL_FILTERING, filter);
    job.setReducerClass(PageRankDbMerger.class);

    FileOutputFormat.setOutputPath(job, newPageRankDb);
    job.setOutputFormat(MapFileOutputFormat.class);
    job.setBoolean("mapred.output.compress", true);
    job.setOutputKeyClass(Text.class);

    // DIFF: Use IntWritable instead of Inlinks as the output value type.
    job.setOutputValueClass(IntWritable.class);

    return job;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws Exception 
  {
    int res = ToolRunner.run(NutchConfiguration.create(), new PageRankDbMerger(), args);
    System.exit(res);
  }
  
  public int run(String[] args) throws Exception 
  {
    if (args.length < 2) 
      {
        System.err.println("Usage: PageRankDbMerger <output_pagerankdb> <pagerankdb1> [<pagerankdb2> <pagerankdb3> ...] [-normalize] [-filter]");
        System.err.println("\toutput_pagerankdb\toutput PageRankDb");
        System.err.println("\tpagerankdb1 ...\tinput PageRankDb-s (single input PageRankDb is ok)");
        System.err.println("\t-normalize\tuse URLNormalizer on both fromUrls and toUrls in pagerankdb(s) (usually not needed)");
        System.err.println("\t-filter\tuse URLFilters on both fromUrls and toUrls in pagerankdb(s)");
        return -1;
      }
    Path output = new Path(args[0]);
    ArrayList<Path> dbs = new ArrayList<Path>();
    boolean normalize = false;
    boolean filter = false;
    for (int i = 1; i < args.length; i++) 
      {
        if (args[i].equals("-filter")) 
          {
            filter = true;
          } else if (args[i].equals("-normalize")) 
          {
            normalize = true;
          } else dbs.add(new Path(args[i]));
      }
    try 
      {
        merge(output, dbs.toArray(new Path[dbs.size()]), normalize, filter);
        return 0;
      } 
    catch (Exception e) 
      {
        LOG.fatal("PageRankDbMerger: " + StringUtils.stringifyException(e));
        return -1;
      }
  }

}
