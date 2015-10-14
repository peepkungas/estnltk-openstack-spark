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
 * <p>This is a one-off/hack to (hopefully) efficiently combine
 * multiple "parse_text/part-nnnnn" map files into a single map file.
 * Using the Nutch 'mergesegs' takes far too long in practice, and
 * often fails to complete due to memory constraints.
 * </p>
 * <p>This class takes advantage of the fact that the
 * "parse_text/part-nnnnn" directories are Hadoop MapFiles.  To merge
 * them, all we have to do is read key/value pairs from each one and
 * write them back out in sorted order.
 * </p>
 */
public class ParseTextCombiner extends Configured implements Tool 
{
  public static final Log LOG = LogFactory.getLog(ParseTextCombiner.class);
  
  private boolean verbose = false;

  public ParseTextCombiner()
  {
    
  }
  
  public ParseTextCombiner(Configuration conf)
  {
    setConf(conf);
  }
  
  /** 
   * Create an index for the input files in the named directory. 
   */
  public static void main(String[] args)
    throws Exception
  {
    int res = ToolRunner.run(NutchConfiguration.create(), new ParseTextCombiner(), args);
    System.exit(res);
  }

  /**
   *
   */
  public int run(String[] args) 
    throws Exception
  {
    String usage = "Usage: ParseTextCombiner [-v] output input...\n";

    if ( args.length < 1 )
      {
        System.err.println( "Usage: " + usage );
        return 1;
      }

    if ( args[0].equals( "-h" ) )
      {
        System.err.println( "Usage: " + usage );
        return 1;
      }

    int argStart = 0;
    if ( args[argStart].equals( "-v" ) )
      {
        verbose = true;
        argStart = 1;
      }

    if ( args.length - argStart < 2 )
      {
        System.err.println( "Usage: " + usage );
        return 1;
      }

    Configuration conf = getConf( );
    FileSystem    fs   = FileSystem.get( conf );

    Path outputPath = new Path( args[argStart] );
    if ( fs.exists( outputPath ) )
      {
        System.err.println( "ERROR: output already exists: " + outputPath );
        return -1;
      }

    MapFile.Reader[] readers = new MapFile.Reader[args.length - argStart - 1];
    for ( int pos = argStart + 1 ; pos < args.length ; pos++ )
      {
        readers[pos - argStart - 1] = new MapFile.Reader( fs, args[pos], conf );
      }

    WritableComparable[] keys   = new WritableComparable[readers.length];
    Writable[]           values = new Writable          [readers.length];

    WritableComparator wc = WritableComparator.get( (Class<WritableComparable>) readers[0].getKeyClass() );

    MapFile.Writer writer = new MapFile.Writer( conf, fs, outputPath.toString(), (Class<WritableComparable>) readers[0].getKeyClass(), readers[0].getValueClass( ) );

    int readCount  = 0;
    int writeCount = 0;

    for ( int i = 0 ; i < readers.length ; i++ )
      {
        WritableComparable key   = (WritableComparable) ReflectionUtils.newInstance( readers[i].getKeyClass(),   conf );
        Writable           value = (Writable)           ReflectionUtils.newInstance( readers[i].getValueClass(), conf );
        
        if ( readers[i].next( key, value ) )
          {
            keys  [i] = key;
            values[i] = value;
            
            readCount++;
            if ( verbose ) System.out.println( "read: " + i + ": " + key );
          }
        else
          {
            // Not even one key/value pair in the map.
            System.out.println( "WARN: No key/value pairs in mapfile: " + args[i+argStart+1] );
            try { readers[i].close(); } catch ( IOException ioe ) { /* Don't care */ }
            readers[i] = null;
          }
      }

    while ( true )
      {
        int candidate = -1;

        for ( int i = 0 ; i < keys.length ; i++ )
          {
            if ( keys[i] == null ) continue ;

            if ( candidate < 0 )
              {
                candidate = i;
              }
            else if ( wc.compare( keys[i], keys[candidate] ) < 0 )
              {
                candidate = i;
              }
          }

        if ( candidate < 0 )
          {
            if ( verbose ) System.out.println( "Candidate < 0, all done." );
            break ;
          }
        
        // Candidate is the index of the "smallest" key.

        // Write it out.
        writer.append( keys[candidate], values[candidate] );
        writeCount++;
        if ( verbose ) System.out.println( "write: " + candidate + ": " + keys[candidate] );

        // Now read in a new value from the corresponding reader.
        if ( ! readers[candidate].next( keys[candidate], values[candidate] ) )
          {
            if ( verbose ) System.out.println( "No more key/value pairs in (" + candidate + "): " + args[candidate+argStart+1] );
            
            // No more key/value pairs left in this reader.
            try { readers[candidate].close(); } catch ( IOException ioe ) { /* Don't care */ }
            readers[candidate] = null;
            keys   [candidate] = null;
            values [candidate] = null;
          }
        else
          {
            readCount++;
            if ( verbose ) System.out.println( "read: " + candidate + ": " + keys[candidate] );
          }
      }

    System.out.println( "Total # records in : " + readCount  );
    System.out.println( "Total # records out: " + writeCount ); 
    
    writer.close();

    return 0;
  }
}
