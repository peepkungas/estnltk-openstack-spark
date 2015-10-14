/*
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
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.nutch.crawl.NutchWritable;
import org.apache.nutch.indexer.IndexerOutputFormat;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilters;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;

public class IndexerMapReduce extends Configured
implements Mapper<Text, Writable, Text, NutchWritable>,
          Reducer<Text, NutchWritable, Text, NutchDocument> {

  public static final Log LOG = LogFactory.getLog(IndexerMapReduce.class);

  private IndexingFilters filters;

  public void configure(JobConf job) {
    setConf(job);
    this.filters = new IndexingFilters(getConf());
  }

  public void map(Text key, Writable value,
      OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {
    output.collect(key, new NutchWritable(value));
  }

  public void reduce(Text key, Iterator<NutchWritable> values,
                     OutputCollector<Text, NutchDocument> output, Reporter reporter)
    throws IOException {
    ParseData parseData = null;
    ParseText parseText = null;
    while (values.hasNext()) {
      final Writable value = values.next().get(); // unwrap

      if (value instanceof ParseData) {
        parseData = (ParseData)value;
      } else if (value instanceof ParseText) {
        parseText = (ParseText)value;
      } else if (LOG.isWarnEnabled()) {
        LOG.warn("Unrecognized type: "+value.getClass());
      }
    }

    if ( parseText == null || parseData == null ) {
      return;
    }

    NutchDocument doc = new NutchDocument();
    final Metadata metadata = parseData.getContentMeta();

    if ( metadata.get(Nutch.SEGMENT_NAME_KEY) == null ||
         metadata.get(Nutch.SIGNATURE_KEY)    == null    )
      {
        LOG.warn( "Skipping document, insufficient metadata: key=" + key + " metadata=" + metadata );
        return ;
      }

    // add segment, used to map from merged index back to segment files
    doc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));

    // add digest, used by dedup
    doc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));

    final Parse parse = new ParseImpl(parseText, parseData);
    try {
      // run indexing filters
      doc = this.filters.filter(doc, parse, key, /*fetchDatum*/ null, /*inlinks*/ null);
    } catch (final IndexingException e) {
      if (LOG.isWarnEnabled()) { LOG.warn("Error indexing "+key+": "+e); }
      return;
    }

    // skip documents discarded by indexing filters
    if (doc == null) return;

    doc.setScore( 1.0f );

    output.collect(key, doc);
  }

  public void close() throws IOException { }

  public static void initMRJob(Collection<Path> segments,
                           JobConf job) {

    for (final Path segment : segments) {
      LOG.info("IndexerMapReduces: adding segment: " + segment);
      FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
      FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
    }

    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(IndexerMapReduce.class);
    job.setReducerClass(IndexerMapReduce.class);

    job.setOutputFormat(IndexerOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NutchWritable.class);
    job.setOutputValueClass(NutchWritable.class);
  }
}
