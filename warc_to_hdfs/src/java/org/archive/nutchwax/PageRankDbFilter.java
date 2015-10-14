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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.nutch.net.URLFilters;
import org.apache.nutch.net.URLNormalizers;

/**
 * <p>This class provides a way to separate the URL normalization
 * and filtering steps from the rest of LinkDb manipulation code.</p>
 * <p>Aaron Binns @ archive.org: see comments in PageRankDbMerger.</p>
 * 
 * @author Andrzej Bialecki
 * @author Aaron Binns (archive.org)
 */
public class PageRankDbFilter implements Mapper<Text, IntWritable, Text, IntWritable>
{
  public static final String URL_FILTERING = "linkdb.url.filters";
  
  public static final String URL_NORMALIZING = "linkdb.url.normalizer";

  public static final String URL_NORMALIZING_SCOPE = "linkdb.url.normalizer.scope";

  private boolean filter;

  private boolean normalize;

  private URLFilters filters;

  private URLNormalizers normalizers;
  
  private String scope;
  
  public static final Log LOG = LogFactory.getLog(PageRankDbFilter.class);

  private Text newKey = new Text();
  
  public void configure(JobConf job)
  {
    filter = job.getBoolean(URL_FILTERING, false);
    normalize = job.getBoolean(URL_NORMALIZING, false);
    if (filter)
      {
        filters = new URLFilters(job);
      }
    if (normalize)
      {
        scope = job.get(URL_NORMALIZING_SCOPE, URLNormalizers.SCOPE_LINKDB);
        normalizers = new URLNormalizers(job, scope);
      }
  }

  public void close()
  {
  }

  public void map(Text key, IntWritable value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException
  {
    String url = key.toString();
    // Inlinks result = new Inlinks();
    if (normalize)
      {
        try
          {
            url = normalizers.normalize(url, scope); // normalize the url
          }
        catch (Exception e)
          {
            LOG.warn("Skipping " + url + ":" + e);
            url = null;
          }
      }
    if (url != null && filter)
      {
        try
          {
            url = filters.filter(url); // filter the url
          }
        catch (Exception e)
          {
            LOG.warn("Skipping " + url + ":" + e);
            url = null;
          }
      }
    if (url == null) return; // didn't pass the filters

    // DIFF: Now that normalizers and filters have run, just emit the
    //       <url,value> pair.  No processing to be done on the value.
    Text newKey = new Text( url );
    output.collect( newKey, value );
  }
}
