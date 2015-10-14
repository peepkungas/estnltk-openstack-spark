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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.nutch.util.NutchConfiguration;


/**
 * A nice command-line hack to generate a Lucene index N documents,
 * each with one field set to the same value.  This value is both
 * stored and tokenized/indexed.
 */
public class BuildIndex extends Configured implements Tool
{
  public int run( String[] args ) throws Exception
  {
    if ( args.length < 4 )
      {
        System.out.println( "BuildIndex index field value count" );
        System.exit( 0 );
      }

    String indexDir   = args[0].trim();
    String fieldKey   = args[1].trim();
    String fieldValue = args[2].trim();
    int    count      = Integer.parseInt( args[3].trim() );
    
    IndexWriter writer = new IndexWriter( indexDir, new WhitespaceAnalyzer( ), true );
    
    for ( int i = 0 ; i < count ; i++ )
      {
        Document newDoc = new Document( );
        newDoc.add( new Field( fieldKey, fieldValue, Field.Store.YES, Field.Index.TOKENIZED ) );

        writer.addDocument( newDoc );
      }

    writer.close( );

    return 0;
  }

  /**
   * Runs using the Hadoop ToolRunner, which means it accepts the
   * standard Hadoop command-line options.
   */
  public static void main( String args[] ) throws Exception
  {
    int result = ToolRunner.run( NutchConfiguration.create(), new BuildIndex(), args );

    System.exit( result );
  }
  
}
