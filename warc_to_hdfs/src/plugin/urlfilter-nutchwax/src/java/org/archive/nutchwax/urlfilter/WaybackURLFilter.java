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
package org.archive.nutchwax.urlfilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.net.URLFilter;

import org.archive.wayback.UrlCanonicalizer;

/** 
 * Nutch URLFilter that filters a URL based on URL+digest+date
 * metadata values, where the URL can also be canonicalized using the
 * same logic as the Wayback.  By making Wayback canonicalization
 * available, we can use exclusion rules generated from CDX files.
 */
public class WaybackURLFilter implements URLFilter
{
  public static final Log LOG = LogFactory.getLog( WaybackURLFilter.class );

  private Configuration    conf;
  private UrlCanonicalizer canonicalizer;
  private Set<String>      exclusions;

  public WaybackURLFilter( )
  {
  }

  /**
   * 
   */
  public String filter( String urlString )
  {
    // Assume input is in expected form of space-separated values
    //   url
    //   digest
    //   14-digit timestamp
    String s[] = urlString.split( "\\s+" );

    if ( s.length != 3 )
      {
        // Don't filter.
        LOG.info( "Allowing : " + urlString );

        return urlString;
      }

    boolean exclude = false;

    String url    = s[0];
    String digest = s[1];
    String date   = s[2];

    try
      {
        // First, transform the URL into the same form that the
        // Wayback uses for CDX files.
        url = this.canonicalizer.urlStringToKey( url );
        
        // Then, build a key to be compared against the exclusion
        // list.
        String key = url + digest + date;

        exclude = this.exclusions.contains( key );
      }
    catch ( URIException e )
      {
        // If we can't handle the URL, we let it through.
        exclude = false;
      }

    if ( exclude )
      {
        LOG.info( "Excluding: " + urlString );

        return null;
      }

    LOG.info( "Allowing : " + urlString );

    return urlString;
  }

  public Configuration getConf( )
  {
    return conf;
  }
  
  public void setConf( Configuration conf )
  {
    this.conf = conf;

    this.canonicalizer = getCanonicalizer( conf );
    this.exclusions    = getExclusions   ( conf );
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
   * Utility function to read a list of exclusion records from a file
   * specified in the configuration.
   */
  public static Set<String> getExclusions( Configuration conf )
  {
    String exclusionsPath = conf.get( "nutchwax.urlfilter.wayback.exclusions" );

    if ( exclusionsPath == null || exclusionsPath.trim().length() == 0 )
      {
        LOG.warn( "No exclusions file set for property: \"nutchwax.urlfilter.wayback.exclusions\"" );

        return Collections.EMPTY_SET;
      }

    LOG.warn( "Using exclusions: " + exclusionsPath );

    Set<String> exclusions = new HashSet<String>( );

    BufferedReader reader = null;
    try
      {
        Path p = new Path( exclusionsPath.trim() );
        
        FileSystem fs = FileSystem.get( conf );
        
        if ( fs.exists( p ) )
          {
            InputStream is = p.getFileSystem( conf ).open( p );
            
            reader = new BufferedReader( new InputStreamReader( is, "UTF-8" ) );
            
            String line;
            while ( (line = reader.readLine()) != null )
              {
                String fields[] = line.split( "\\s+" );
                
                if ( fields.length < 3 )
                  {
                    LOG.warn( "Malformed exclusion, not enough fields ("+fields.length+"): " + line );
                    continue ;
                  }

                // We only want the first three fields.  Chop-off anything extra.
                if ( fields.length >= 3 )
                  {
                    line = fields[0] + fields[1] + fields[2];
                  }

                exclusions.add( line );
              }
          }
        else
          {
            LOG.warn( "Exclusions doesn't exist: " + exclusionsPath );
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

    return exclusions;
  }

}
