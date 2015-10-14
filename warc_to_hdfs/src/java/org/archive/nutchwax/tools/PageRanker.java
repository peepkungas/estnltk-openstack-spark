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

package org.archive.nutchwax.tools;

import java.io.*;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.LogUtil;
import org.apache.nutch.util.NutchConfiguration;

import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexWriter;

/**
 *
 */
public class PageRanker extends Configured implements Tool 
{
  public static final Log LOG = LogFactory.getLog(PageRanker.class);

  public PageRanker()
  {
    
  }
  
  public PageRanker(Configuration conf)
  {
    setConf(conf);
  }
  
  /** 
   * Create an index for the input files in the named directory. 
   */
  public static void main(String[] args)
    throws Exception
  {
    int res = ToolRunner.run(NutchConfiguration.create(), new PageRanker(), args);
    System.exit(res);
  }

  /**
   *
   */
  public int run(String[] args) 
    throws Exception
  {
    String usage = "Usage: PageRanker [OPTIONS] outputFile <linkdb|paths>\n"
      + "Emit PageRank values for URLs in linkDb(s).  Suitable for use with\n"
      + "PageRank scoring filter.\n"
      + "\n"
      + "OPTIONS:\n"
      + "  -p              Use exact path as given, don't assume it's a typical\n"
      + "                    linkdb with \"current/part-nnnnn\" subdirs.\n"
      + "  -t threshold    Do not emit records with less than this many inlinks.\n"
      + "                    Default value 10."
      ;
    if ( args.length < 1 )
      {
        System.err.println( "Usage: " + usage );
        return -1;
      }

    boolean exactPath  = false;
    int     threshold  = 10;

    int pos = 0;
    for ( ; pos < args.length && args[pos].charAt(0) == '-' ; pos++ )
      {
        if ( args[pos].equals( "-p" ) )
          {
            exactPath = true;
          }
        if ( args[pos].equals( "-t" ) ) 
          {
            pos++;
            if ( args.length - pos < 1 ) 
              {
                System.err.println( "Error: missing argument to -t option" );
                return -1;
              }
            try
              {
                threshold = Integer.parseInt( args[pos] );
              }
            catch ( NumberFormatException nfe )
              {
                System.err.println( "Error: bad value for -t option: " + args[pos] );
                return -1;
              }
          }
      }

    Configuration conf = getConf( );
    FileSystem    fs   = FileSystem.get( conf );

    if ( pos >= args.length )
      {
        System.err.println( "Error: missing outputFile" );
        return -1;
      }

    Path outputPath = new Path( args[pos++] );
    if ( fs.exists( outputPath ) )
      {
        System.err.println( "Erorr: outputFile already exists: " + outputPath );
        return -1;
      }

    if ( pos >= args.length )
      {
        System.err.println( "Error: missing linkdb" );
        return -1;
      }

    List<Path> mapfiles = new ArrayList<Path>();

    // If we are using exact paths, add each one to the list.
    // Otherwise, assume the given path is to a linkdb and look for
    // <linkdbPath>/current/part-nnnnn sub-dirs.
    if ( exactPath )
      {
        for ( ; pos < args.length ; pos++ )
          {
            mapfiles.add( new Path( args[pos] ) );
          }
      }
    else
      {
        for ( ; pos < args.length ; pos++ )
          {
            FileStatus[] fstats = fs.listStatus( new Path(args[pos]+"/current"), HadoopFSUtil.getPassDirectoriesFilter(fs));
            mapfiles.addAll(Arrays.asList(HadoopFSUtil.getPaths(fstats)));
          }
      }

    System.out.println( "mapfiles = " + mapfiles );

    PrintWriter output = new PrintWriter( new OutputStreamWriter( fs.create( outputPath ).getWrappedStream( ), "UTF-8" ) );

    try 
      {
        for ( Path p : mapfiles )
          {
            MapFile.Reader reader = new MapFile.Reader( fs, p.toString(), conf );
            
            WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance( reader.getKeyClass()  , conf );
            Writable           value = (Writable)           ReflectionUtils.newInstance( reader.getValueClass(), conf );
            
            while ( reader.next( key, value ) )
              {
                if ( ! (key instanceof Text) ) continue ;

                String toUrl = ((Text) key).toString( );

                // HACK: Should make this into some externally configurable regex.
                if ( ! toUrl.startsWith( "http" ) ) continue;

                int count = -1;
                if ( value instanceof IntWritable )
                  {
                    count = ( (IntWritable) value ).get( );
                  }
                else if ( value instanceof Inlinks )
                  {
                    Inlinks inlinks = (Inlinks) value;

                    count = inlinks.size( );
                  }
                
                if ( count < threshold ) continue ;

                output.println( count + " " + toUrl );
              }
          }

        return 0;
      }
    catch ( Exception e )
      {
        LOG.fatal( "PageRanker: " + StringUtils.stringifyException( e ) );
        return -1;
      }
    finally
      {
        output.flush( );
        output.close( );
      }
  }
}
