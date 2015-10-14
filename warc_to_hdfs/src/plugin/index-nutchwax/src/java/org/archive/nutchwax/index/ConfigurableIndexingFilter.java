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
package org.archive.nutchwax.index;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.nutch.indexer.lucene.LuceneWriter.INDEX;
import org.apache.nutch.indexer.lucene.LuceneWriter.STORE;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;

/** 
 * Adds web archive specific fields to a document.
 */
public class ConfigurableIndexingFilter implements IndexingFilter
{
  public static final Log LOG = LogFactory.getLog( ConfigurableIndexingFilter.class );

  private Configuration conf;
  private List<FieldSpecification> fieldSpecs;

  private int MAX_TITLE_LENGTH;

  public void setConf( Configuration conf )
  {
    this.conf = conf;

    this.MAX_TITLE_LENGTH = conf.getInt("indexer.max.title.length", 100);
    
    String filterSpecs = conf.get( "nutchwax.filter.index" );
    
    if ( null == filterSpecs )
      {
        return ;
      }

    this.fieldSpecs = new ArrayList<FieldSpecification>( );

    filterSpecs = filterSpecs.trim( );
    
    for ( String filterSpec : filterSpecs.split("\\s+") )
      {
        String spec[] = filterSpec.split("[:]");

        String  srcKey     = spec[0];
        boolean lowerCase  = true;
        STORE   store      = STORE.YES;
        INDEX   index      = INDEX.TOKENIZED;
        boolean exclusive  = true;
        String  destKey    = srcKey;
        switch ( spec.length )
          {
          default:
          case 6:
            destKey   = spec[5];
          case 5:
            exclusive = Boolean.parseBoolean( spec[4] );
          case 4:
            index     = "tokenized".  equals(spec[3]) ? INDEX.TOKENIZED : 
                        "untokenized".equals(spec[3]) ? INDEX.UNTOKENIZED : 
                        "no_norms".   equals(spec[3]) ? INDEX.NO_NORMS :
                        INDEX.NO;
          case 3:
            //store     = Boolean.parseBoolean( spec[2] );
            store     = "true".    equals(spec[2]) ? STORE.YES :
                        "compress".equals(spec[2]) ? STORE.COMPRESS :
                        STORE.NO;
          case 2:
            lowerCase = Boolean.parseBoolean( spec[1] );
          case 1:
            // Nothing to do
            ;
          }

        LOG.info( "Add field specification: " + srcKey + ":" + lowerCase + ":" + store + ":" + index + ":" + exclusive + ":" + destKey );

        this.fieldSpecs.add( new FieldSpecification( srcKey, lowerCase, store, index, exclusive, destKey ) );
      }
  }

  private static class FieldSpecification
  {
    String  srcKey;
    boolean lowerCase;
    STORE   store;
    INDEX   index;
    boolean exclusive;
    String  destKey;

    public FieldSpecification( String srcKey, boolean lowerCase, STORE store, INDEX index, boolean exclusive, String destKey )
    {
      this.srcKey    = srcKey;
      this.lowerCase = lowerCase;
      this.store     = store;
      this.index     = index;
      this.exclusive = exclusive;
      this.destKey   = destKey;
    }
  }

  public Configuration getConf()
  {
    return this.conf;
  }

  /**
   * Transfer NutchWAX field values stored in the parsed content to
   * the Lucene document.
   */
  public NutchDocument filter( NutchDocument doc, Parse parse, Text key, CrawlDatum datum, Inlinks inlinks )
    throws IndexingException
  {
    Metadata meta = parse.getData().getContentMeta();

    for ( FieldSpecification spec : this.fieldSpecs )
      {
        String value = null;
        if ( "site".equals( spec.srcKey ) || "host".equals( spec.srcKey ) )
          {
            try
              {
                value = (new URL( meta.get( "url" ) ) ).getHost( );

                // Strip off any "www." header.
                if ( value.startsWith( "www." ) )
                  {
                    value = value.substring( 4 );
                  }
              }
            catch ( MalformedURLException mue ) { /* Eat it */ }
          }
        else if ( "content".equals( spec.srcKey ) ) 
          {
            value = parse.getText( );
          }
        else if ( "title".equals( spec.srcKey ) )
          {
            value = parse.getData().getTitle();
            if ( value.length() > MAX_TITLE_LENGTH )      // truncate title if needed
              {
                value = value.substring( 0, MAX_TITLE_LENGTH );
              }
          }
        else if ( "type".equals( spec.srcKey ) )
          {
            value = meta.get( spec.srcKey );

            if ( value == null ) continue ;

            int p = value.indexOf( ';' );
            if ( p >= 0 ) value = value.substring( 0, p );
          }
        else if ( "collection".equals( spec.srcKey ) )
          {
            // Use value given in config first, otherwise what's in the metadata object.
            value = conf.get( "nutchwax.index.collection", meta.get( spec.srcKey ) );
          }
        else
          {
            value = meta.get( spec.srcKey );
          }
        
        if ( value == null ) continue;

        if ( spec.lowerCase )
          {
            value = value.toLowerCase( );
          }

        if ( spec.exclusive )
          {
            doc.removeField( spec.destKey );
          }

        if ( spec.store != STORE.NO || spec.index != INDEX.NO )
          {
            doc.add( spec.destKey, value );
          }

      }

    return doc;
  }
 
  public void addIndexBackendOptions( Configuration conf ) 
  {
    for ( FieldSpecification spec : this.fieldSpecs )
      {
        if ( spec.store == STORE.NO && spec.index == INDEX.NO )
          {
            continue ;
          }

        LuceneWriter.addFieldOptions( spec.destKey, 
                                      spec.store,
                                      spec.index,
                                      conf );
      }

  }
}
