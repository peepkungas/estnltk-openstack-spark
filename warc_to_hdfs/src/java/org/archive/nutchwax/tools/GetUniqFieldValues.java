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

import java.io.File;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;

/**
 * A quick-n-dirty command-line utility to get the unique values for a
 * field in an index and print them to stdout.
 */
public class GetUniqFieldValues
{
  public static void main(String[] args) throws Exception
  {
    String fieldName = "";
    String indexDir  = "";

    if ( args.length == 2 )
      {
        fieldName = args[0];
        indexDir  = args[1];
      }
    
    if (! (new File(indexDir)).exists())
      {
        usageAndExit();
      }
    
    dumpUniqValues( fieldName, indexDir );
  }
  
  private static void dumpUniqValues( String fieldName, String indexDir ) throws Exception
  {
    IndexReader reader = IndexReader.open(indexDir);
    
    Collection fieldNames = reader.getFieldNames( IndexReader.FieldOption.ALL );

    if ( ! fieldNames.contains( fieldName ) )
      {
        System.out.println( "Field not in index: " + fieldName );
        System.exit( 2 );
      }

    int numDocs = reader.numDocs();
    Set<String> values = new HashSet<String>( );
    
    for ( int i = 0; i < numDocs; i++ )
      {
        values.add( reader.document(i).get( fieldName ) );
      }
    
    for ( String v : values )
      {
        System.out.println( v );
      }

  }
  
  private static void usageAndExit()
  {
    System.out.println("Usage: GetUniqFieldValues field index");
    System.exit(1);
  }
}
