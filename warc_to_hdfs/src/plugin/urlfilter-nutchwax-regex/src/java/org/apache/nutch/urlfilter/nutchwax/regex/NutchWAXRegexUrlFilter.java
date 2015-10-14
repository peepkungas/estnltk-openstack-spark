/*
 * Kaarel T\u00F5nisson 2015.
 * This code is heavily based on WaybackURLFilter.
 * 
 * 
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

package org.apache.nutch.urlfilter.nutchwax.regex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.net.URLFilter;

/** 
 * NutchWAX URLFilter that filters by regex, like the one for the original Nutch.
 * Code composed of parts from WaybackURLFilter and RegexURLFilterBase.
 */
public class NutchWAXRegexUrlFilter implements URLFilter
{
	public static final Log LOG = LogFactory.getLog( NutchWAXRegexUrlFilter.class );

	private Configuration    conf;
	private Set<String>      plusregexes;
	private Set<String>      minusregexes;

	/**
	 * Apply filtering based on regexes described in file set in config
	 * parameter nutchwax.urlfilter.regex.exclusions 
	 * @return urlString if input matches all regexes, otherwise null.
	 */
	public String filter( String urlString )
	{
		String s[] = urlString.split( "\\s+" );
		String url = s[0];

		for (String regex : minusregexes){
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(url);
			boolean isMatch = matcher.matches();
			if (isMatch){
//				LOG.info( "Excluding: " + url + " based on regex:" + regex );
				return null;
			}
		}
		
		for (String regex : plusregexes){
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(url);
			boolean isMatch = matcher.matches();
			if (isMatch){
//				LOG.info( "Allowing: " + url + " based on regex:" + regex);
				return urlString;
			}
		}
//		LOG.info("No mathching regex, allowing: " + url);
		return urlString;
	}


	/**
	 * Obtains regexes described in config parameter and extracts them.
	 * @return nothing. Values are saved to variables.
	 */
	private List<String> getRegexesFromFile() {
		String exclusionsPath = conf.get( "nutchwax.urlfilter.regex.exclusions" );
		if ( exclusionsPath == null || exclusionsPath.trim().length() == 0 )
		{
			LOG.warn( "No regex file set for property: \"nutchwax.urlfilter.regex.exclusions\"" );

			return null;
		}
		plusregexes = new HashSet<String>();
		minusregexes = new HashSet<String>();
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
					if (line.length() == 0) {
				        continue;
				      }
				      char first=line.charAt(0);
				      switch (first) {
				      case '+' : 
				    	  plusregexes.add( line.substring(1) );
				        break;
				      case '-' :
				    	  minusregexes.add( line.substring(1) );
				        break;
				      case ' ' : case '\n' : case '#' :           // skip blank & comment lines
				        continue;
				      default :
				        throw new IOException("Invalid first character: " + line);
				      }
					
				}
			}
			else
			{
				LOG.warn( "Regex file doesn't exist: " + exclusionsPath );
			}
		}
		catch ( IOException e )
		{
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
		
		return null;
	}



	public Configuration getConf( )
	{
		return conf;
	}

	public void setConf( Configuration conf )
	{
		this.conf = conf;
		getRegexesFromFile();
		
	}

}


