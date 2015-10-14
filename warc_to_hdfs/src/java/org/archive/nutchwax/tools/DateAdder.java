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
package org.archive.nutchwax.tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.WhitespaceAnalyzer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.nutch.util.NutchConfiguration;

import org.archive.wayback.UrlCanonicalizer;

import org.archive.nutchwax.NutchWax;


/**
 * Reads series of (digest+URL,date) lines, finds corresponding
 * document in index, and adds the date to it.
 */
public class DateAdder extends Configured implements Tool
{
  public int run( String[] args ) throws Exception
  {
    if ( args.length < 4 )
      {
        System.out.println( "DateAdder <key-index> <source1> ... <sourceN> <dest> <records>" );
        System.exit( 0 );
      }

    String mainIndexDir = args[0].trim();
    String destIndexDir = args[args.length - 2].trim();
    String recordsFile  = args[args.length - 1].trim();
    
    InputStream recordsStream;
    if ( "-".equals( recordsFile ) )
      {
        recordsStream = System.in;
      }
    else
      {
        recordsStream = new FileInputStream( recordsFile );
      }

    // Read date-addition records from stdin.
    Map<String,String> dateRecords = new HashMap<String,String>( );
    BufferedReader br = new BufferedReader( new InputStreamReader( recordsStream, "UTF-8" ) );
    String line;
    while ( (line = br.readLine( )) != null )
      {
        String fields[] = line.split("\\s+");
        if ( fields.length < 3 ) 
          {
            System.out.println( "Malformed line, not enough fields (" + fields.length +"): " + line );
            continue;
          }

        // Key is hash+url, value is String which is a " "-separated list of dates
        String key   = fields[0] + fields[1];
        String dates = dateRecords.get( key );
        if ( dates != null )
          {
            dates += " " + fields[2];
            dateRecords.put( key, dates );
          }
        else
          {
            dateRecords.put( key , fields[2] );
          }

      }

    IndexReader reader = IndexReader.open( mainIndexDir );
    
    IndexReader sourceReaders[] = new IndexReader[args.length-3];
    for ( int i = 0 ; i < sourceReaders.length ; i++ )
      {
        sourceReaders[i] = IndexReader.open( args[i+1] );
      }

    IndexWriter writer = new IndexWriter( destIndexDir, new WhitespaceAnalyzer( ), true );
    
    UrlCanonicalizer canonicalizer = getCanonicalizer( this.getConf( ) );

    for ( int i = 0 ; i < reader.numDocs( ) ; i++ )
      {
        Document oldDoc = reader.document( i );
        Document newDoc = new Document( );

        // Copy the values from all the source indices to the new
        // document.
        Set<String> uniqueDates = new HashSet<String>( );
        for ( IndexReader source : sourceReaders )
          {
            Document sourceDoc = source.document( i );
            
            String dates[] = sourceDoc.getValues( NutchWax.DATE_KEY );

            Collections.addAll( uniqueDates, dates );
          }
        for ( String date : uniqueDates )
          {
            newDoc.add( new Field( NutchWax.DATE_KEY, date, Field.Store.YES, Field.Index.UN_TOKENIZED ) );
          }

        // Obtain the new dates for the document.
        String newDates = null;
        try
          {
            // First, apply URL canonicalization from Wayback
            String canonicalizedUrl = canonicalizer.urlStringToKey( oldDoc.get( NutchWax.URL_KEY ) );

            // Now, get the digest+URL of the document, look for it in
            // the updateRecords and if found, add the date.
            String key = canonicalizedUrl + oldDoc.get( NutchWax.DIGEST_KEY );
            
            newDates = dateRecords.get( key );
          }
        catch ( Exception e )
          {
            // The canonicalizer can throw various types of exceptions
            // due to malformed URIs.
            System.err.println( "WARN: Not adding dates on malformed URI: " + oldDoc.get( NutchWax.URL_KEY ) );
          }

        // If there are any new dates, add them to the new document.
        if ( newDates != null )
          {
            for ( String date : newDates.split("\\s+") )
              {
                newDoc.add( new Field( NutchWax.DATE_KEY, date, Field.Store.YES, Field.Index.UN_TOKENIZED ) );
              }
          }

        // Finally, add the new document to the new index.
        writer.addDocument( newDoc );
      }

    reader.close( );
    writer.close( );

    return 0;
  }

  /**
   * Utility function to instantiate a UrlCanonicalizer based on an
   * implementation specified in the configuration.
   */
  public static UrlCanonicalizer getCanonicalizer( Configuration conf )
  {
    // Which Wayback canonicalizer to use: Aggressive, Identity, etc.
    String canonicalizerClassName = conf.get( "nutchwax.urlfilter.wayback.canonicalizer" );

    if ( canonicalizerClassName == null || canonicalizerClassName.trim().length() == 0 )
      {
        throw new RuntimeException( "Missing value for property: nutchwax.urlfilter.wayback.canonicalizer" );
      }

    try
      {
        UrlCanonicalizer canonicalizer = (UrlCanonicalizer) Class.forName( canonicalizerClassName ).newInstance( );

        return canonicalizer;
      }
    catch ( Exception e )
      {
        // If we can't instantiate it, there's not much else we can do
        // other than just throw the Exception.
        throw new RuntimeException( e );
      }
  }

  /**
   * Command-line driver.  Runs the Importer as a Hadoop job.
   */
  public static void main( String args[] ) throws Exception
  {
    int result = ToolRunner.run( NutchConfiguration.create(), new DateAdder(), args );

    System.exit( result );
  }

  
}
