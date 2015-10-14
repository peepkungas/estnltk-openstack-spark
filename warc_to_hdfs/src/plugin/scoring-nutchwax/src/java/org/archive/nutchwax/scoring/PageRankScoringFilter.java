/*
 * Copyright (C) 2008 Internet Archive.
 * 
 * This file is part of the archive-access tools project
 * (http://sourceforge.net/projects/archive-access).
 * 
 * The archive-access tools are free software; you can redistribute them and/or
 * modify them under the terms of the GNU Lesser Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or any
 * later version.
 * 
 * The archive-access tools are distributed in the hope that they will be
 * useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser Public License along with
 * the archive-access tools; if not, write to the Free Software Foundation,
 * Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.archive.nutchwax.scoring;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.lucene.document.Document;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;


/**
 * Simple scoring plugin that applies a PageRank multiple to the
 * document score/boost during index time.  Only implements the
 * <code>ScoringFilter</code> method associated with indexing, none of
 * the other scoring methods are implemented.
 * </p><p>
 * Applies a simple log10 multipler to the document score based on the
 * base-10 log value of the number of inlinks.  For example, a page with
 * 13,032 inlinks will have a score/boost of 5.115.  The actual formula is
 * </p>
 * <code>
 *  newScore = initialScore * ( log10( # inlinks ) + 1 )
 * </code>
 * <p>
 * We add the extra 1 for pages with only 1 inlink since log10(1)=0 and we
 * don't want a 0 multiplier.
 * </p>
 * <p>
 * The number of inlinks for a page is not taken from the <code>inlinks</code>
 * method parameter.  Rather a map of &lt;URL,rank&gt; values is read from
 * an external file.  Confusing?  Yes.
 * </p>
 * <p>
 * We use an external file because the <code>inlinks</code> will
 * <strong>always</strong> be empty.  This is because the
 * <code>linkdb</code> uses URLs where the <strong>key</strong> is not
 * the URL rather the URL+digest.  Thus the URLs in the
 * <code>linkdb</code> never match the keys and Hadoop doesn't pass
 * in the expected <code>linkdb</code> information.
 * </p>
 * <p>
 * We work around this by using a NutchWAX command-line tool to
 * extract the relevant PageRank information from the
 * <code>linkdb</code> and write to an external file.  We then read
 * that external file here and use the information contained therein.
 * </p>
 * <p>
 * Yes, this is a hassle.  But it's the best we got right now.
 * </p>
 * <h2>Implementation note</h2>
 * <p>
 * Since the scoring plugins are used <em>only</em> during the
 * <code>reduce</code> step during indexing, we delay the
 * initialization of the &lt;URL,rank&gt; map until the first call to
 * the <code>indexerScore</code> method.  This way, we don't spend the
 * effort to read the external file when we are instantiated during
 * <code>map</code> phase.
 * </p>
 */
public class PageRankScoringFilter implements ScoringFilter
{
  public static final Log LOG = LogFactory.getLog( PageRankScoringFilter.class );
  
  private Configuration       conf;
  private Map<String,Integer> ranks;

  public Configuration getConf( )
  {
    return this.conf;
  }

  public void setConf( Configuration conf )
  {
    this.conf = conf;
  }
  
  public void injectedScore(Text url, CrawlDatum datum) 
    throws ScoringFilterException
  {
    // Not implemented
  }
  
  public void initialScore(Text url, CrawlDatum datum) 
    throws ScoringFilterException
  {
    // Not implemented
  }
  
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort) 
    throws ScoringFilterException
  {
    // Not implemented
    return initSort;
  }
  
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) 
    throws ScoringFilterException
  {
    // Not implemented
  }
  
  public void passScoreAfterParsing(Text url, Content content, Parse parse) 
    throws ScoringFilterException
  {
    // Not implemented
  }
  
  public CrawlDatum distributeScoreToOutlinks(Text fromUrl, ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets, CrawlDatum adjust, int allCount) 
    throws ScoringFilterException
  {
    // Not implemented
    return adjust;
  }
  
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum, List<CrawlDatum> inlinked)
    throws ScoringFilterException
  {
    // Not implemented
  }
  
  public float indexerScore(Text key, NutchDocument doc, CrawlDatum dbDatum, CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
    throws ScoringFilterException
  {
    synchronized ( this )
      {
        if ( this.ranks == null )
          {
            this.ranks = getPageRanks( this.conf );
          }
      }

    LOG.info( "PageRankScoringFilter: initScore = " + initScore + " ; key = " + key );

    if ( initScore <= 0 )
      {
        return initScore;
      }

    String keyParts[] = key.toString( ).split( "\\s+", 2 );

    if ( keyParts.length != 2 )
      {
        LOG.warn( "Unexpected URL/key format: " + key );

        return initScore;
      }
    
    String url = keyParts[0];

    Integer rank = this.ranks.get( url );

    if ( rank == null )
      {
        LOG.info( "No rank found for: " + url );

        return initScore;
      }

    float newScore = initScore * (float) ( Math.log10( rank ) + 1 );

    LOG.info( "PageRankScoringFilter: initScore = " + newScore + " ; key = " + key );

    return newScore;
  }
  

  /**
   * Utility function to read a list of page-rank records from a file
   * specified in the configuration.
   */
  public static Map<String,Integer> getPageRanks( Configuration conf )
  {
    String pageranksPath = conf.get( "nutchwax.scoringfilter.pagerank.ranks" );

    if ( pageranksPath == null || pageranksPath.trim().length() == 0 )
      {
        LOG.warn( "No pagerank file set for property: \"nutchwax.scoringfilter.pagerank.ranks\"" );

        return Collections.EMPTY_MAP;
      }

    LOG.warn( "Using pageranks: " + pageranksPath );

    Map<String,Integer> pageranks = new HashMap<String,Integer>( );

    BufferedReader reader = null;
    try
      {
        Path p = new Path( pageranksPath.trim() );
        
        FileSystem fs = FileSystem.get( conf );
        
        if ( fs.exists( p ) )
          {
            InputStream is = p.getFileSystem( conf ).open( p );
            
            reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
            
            String line;
            while ( (line = reader.readLine()) != null )
              {
                String fields[] = line.split( "\\s+" );
                
                if ( fields.length < 2 )
                  {
                    LOG.warn( "Malformed pagerank, not enough fields ("+fields.length+"): " + line );
                    continue ;
                  }

                try
                  {
                    int    rank = Integer.parseInt( fields[0] );
                    String url  = fields[1];

                    if ( rank < 0 )
                      {
                        LOG.warn( "Malformed pagerank, rank less than 0: " + line );
                      }
                    
                    pageranks.put( url, rank );
                  }
                catch ( NumberFormatException nfe )
                  {
                    LOG.warn( "Malformed pagerank, rank not an integer: " + line );
                    continue ;
                  }
              }
          }
        else
          {
            LOG.warn( "Pagerank file doesn't exist: " + pageranksPath );
          }
      }
    catch ( IOException e )
      {
        // Umm, what to do?
        throw new RuntimeException( e );
      }
    finally
      {
        try
          {
            if ( reader != null )
              {
                reader.close( );
              }
          }
        catch  ( IOException e )
          {
            // Ignore it.
          }
      }

    return pageranks;
  }

}
