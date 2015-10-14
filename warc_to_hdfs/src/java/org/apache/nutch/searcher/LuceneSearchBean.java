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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ArchiveParallelReader;
import org.apache.lucene.index.MultiReader;

import org.apache.nutch.indexer.FsDirectory;
import org.apache.nutch.indexer.Indexer;
import org.apache.nutch.util.HadoopFSUtil;


public class LuceneSearchBean implements RPCSearchBean {

  public static final long VERSION = 1L;

  private IndexSearcher searcher;

  private FileSystem fs;

  private Configuration conf;

  /**
   * Construct in a named directory.
   * @param conf
   * @param dir
   * @throws IOException
   */
  public LuceneSearchBean(Configuration conf, Path pindexesDir, Path indexDir, Path indexesDir )
  throws IOException {
    this.conf = conf;
    this.fs = FileSystem.get(this.conf);
    init( pindexesDir, indexDir, indexesDir );
  }

  private void init( Path pindexesDir, Path indexDir, Path indexesDir)
  throws IOException {

    IndexReader reader = getIndexReader( pindexesDir );
    
    if ( reader != null )
      {
        this.searcher = new IndexSearcher( reader, this.conf );
      }
    else
      {
        if (this.fs.exists(indexDir)) {
          LOG.info("opening merged index in " + indexDir);
          this.searcher = new IndexSearcher(indexDir, this.conf);
        } else {
          LOG.info("opening indexes in " + indexesDir);
          
          List<Path> vDirs = new ArrayList<Path>();
          FileStatus[] fstats = fs.listStatus(indexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
          Path[] directories = HadoopFSUtil.getPaths(fstats);
          for(int i = 0; i < directories.length; i++) {
            Path indexdone = new Path(directories[i], Indexer.DONE_NAME);
            if(fs.isFile(indexdone)) {
              vDirs.add(directories[i]);
            }
          }
          
          directories = new Path[ vDirs.size() ];
          for(int i = 0; vDirs.size()>0; i++) {
            directories[i] = vDirs.remove(0);
          }
          
          this.searcher = new IndexSearcher(directories, this.conf);
        }
      }
  }

  public Hits search(Query query, int numHits, String dedupField,
                     String sortField, boolean reverse)
  throws IOException {
    return searcher.search(query, numHits, dedupField, sortField, reverse);
  }

  public String getExplanation(Query query, Hit hit) throws IOException {
    return searcher.getExplanation(query, hit);
  }

  public HitDetails getDetails(Hit hit) throws IOException {
    return searcher.getDetails(hit);
  }

  public HitDetails[] getDetails(Hit[] hits) throws IOException {
    return searcher.getDetails(hits);
  }

  public boolean ping() throws IOException {
    return true;
  }

  public void close() throws IOException {
    if (searcher != null) { searcher.close(); }
    if (fs != null) { fs.close(); }
  }

  public long getProtocolVersion(String protocol, long clientVersion)
  throws IOException {
    return VERSION;
  }


  private IndexReader getIndexReader( Path pindexesDir )
    throws IOException
  {
    /*
    FileSystem fs = FileSystem.get( conf );
    
    Path dir = new Path( conf.get( "searcher.dir", "crawl") ).makeQualified( fs );
    LOG.info( "Looking for Nutch indexes in: " + dir );
    if ( ! fs.exists( dir ) )
      {
        LOG.warn( "Directory does not exist: " + dir );
        LOG.warn( "No Nutch indexes will be found and all queries will return no results." );
        
        return false;
      }

      Path pindexesDir = new Path( dir, "pindexes" ).makeQualified(fs);
    */

    LOG.info( "Looking for NutchWax parallel indexes in: " + pindexesDir );
    if ( ! fs.exists( pindexesDir ) )
      {
        LOG.warn( "Parallel indexes directory does not exist: " + pindexesDir );

        return null;
      }
    
    if ( ! fs.getFileStatus( pindexesDir ).isDir( ) )
      {
        LOG.warn( "Parallel indexes directory is not a directory: " + pindexesDir );

        return null;
      }
    
    FileStatus[] fstats = fs.listStatus(pindexesDir, HadoopFSUtil.getPassDirectoriesFilter(fs));
    Path[] indexDirs    = HadoopFSUtil.getPaths( fstats );
    
    if ( indexDirs.length < 1 )
      {
        LOG.info( "No sub-dirs found in parallel indexes directory: " + pindexesDir );

        return null;
      }
    
    List<IndexReader> readers = new ArrayList<IndexReader>( indexDirs.length );
    
    for ( Path indexDir : indexDirs )
      {
        fstats = fs.listStatus( indexDir, HadoopFSUtil.getPassDirectoriesFilter(fs) );
        Path parallelDirs[] = HadoopFSUtil.getPaths( fstats );
        
        if ( parallelDirs.length < 1 )
          {
            LOG.info( "No sub-directories, skipping: " + indexDir );
            
            continue;
          }
        
        ArchiveParallelReader reader = new ArchiveParallelReader( );
        
        // Sort the parallelDirs so that we add them in order.  Order
        // matters to the ParallelReader.
        Arrays.sort( parallelDirs );
        
        for ( Path p : parallelDirs )
          {
            LOG.info( "Adding reader for: " + p );
            reader.add( IndexReader.open( new FsDirectory( fs, p, false, conf ) ) ); 
          }
        
        readers.add( reader );
      }
    
    if ( readers.size( ) == 0 )
      {
        LOG.warn( "No parallel indexes in: " + pindexesDir );
        
        return null;
      }
    
    MultiReader reader = new MultiReader( readers.toArray( new IndexReader[0] ) );

    return reader;
  }

}
