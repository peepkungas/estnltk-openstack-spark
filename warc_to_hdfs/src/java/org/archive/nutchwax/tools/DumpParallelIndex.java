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
import java.util.Arrays;
import java.util.Collection;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ArchiveParallelReader;

public class DumpParallelIndex
{
  public static void main( String[] args ) throws Exception
  {
    if ( args.length < 1 )
      {
        usageAndExit( );
      }

    int offset = 0;
    if ( args[0].equals( "-l" ) || args[0].equals( "-c" ) )
      {
        offset = 1;
      }
    if ( args[0].equals( "-f" ) )
      {
        if ( args.length < 2 )
          {
            System.out.println( "Error: missing argument to -f\n" );
            usageAndExit( );
          }
        offset = 2;
      }

    String dirs[] = new String[args.length - offset];
    System.arraycopy( args, offset, dirs, 0, args.length - offset );

    ArchiveParallelReader reader = new ArchiveParallelReader( );
    for ( String dir : dirs )
      {
        reader.add( IndexReader.open( dir ) );
      }

    if ( args[0].equals( "-l" ) )
      {
        listFields( reader );
      }
    else if ( args[0].equals( "-c" ) )
      {
        countDocs( reader );
      }
    else if ( args[0].equals( "-f" ) )
      {
        dumpIndex( reader, args[1] );
      }
    else
      {
        dumpIndex( reader );
      }
  }
  
  private static void dumpIndex( IndexReader reader, String fieldName ) throws Exception
  {
    Collection fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL);

    if ( ! fieldNames.contains( fieldName ) )
      {
        System.out.println( "Field not in index: " + fieldName );
        System.exit( 2 );
      }

    int numDocs = reader.numDocs();
    
    for (int i = 0; i < numDocs; i++)
    {
      System.out.println( Arrays.toString( reader.document(i).getValues( (String) fieldName ) ) );
    }
    
  }

  private static void dumpIndex( IndexReader reader ) throws Exception
  {
    Object[] fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL).toArray( );
    Arrays.sort( fieldNames );

    for ( int i = 0; i < fieldNames.length; i++ )
    {
      System.out.print( fieldNames[i] + "\t" );
    }

    System.out.println();

    int numDocs = reader.numDocs();
    
    for (int i = 0; i < numDocs; i++)
    {
      for (int j = 0; j < fieldNames.length; j++)
      {
        System.out.print( Arrays.toString( reader.document(i).getValues((String) fieldNames[j])) + "\t" );
      }
      
      System.out.println();
    }
  }
  
  private static void listFields( IndexReader reader ) throws Exception
  {
    Object[] fieldNames = reader.getFieldNames(IndexReader.FieldOption.ALL).toArray( );
    Arrays.sort( fieldNames );

    for ( int i = 0; i < fieldNames.length; i++ )
    {
      System.out.println( fieldNames[i] );
    }
  }
  
  private static void countDocs( IndexReader reader ) throws Exception
  {
    System.out.println( reader.numDocs( ) );
  }
  
  private static void usageAndExit()
  {
    System.out.println( "Usage: DumpParallelIndex [option] index1 ... indexN" );
    System.out.println( "Options:" );
    System.out.println( "  -c                Emit document count" );
    System.out.println( "  -f <fieldname>    Only dump specified field" );
    System.out.println( "  -l                List fields in index" );
    System.exit(1);
  }
}
