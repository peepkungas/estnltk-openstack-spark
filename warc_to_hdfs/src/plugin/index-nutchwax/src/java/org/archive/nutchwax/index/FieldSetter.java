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

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

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
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.Parse;

/** 
 * <p>Indexing filter that assigns a static value to a field in the Lucene document.
 * It can also be used to remove a field from the Lucene document by specifying
 * an empty value for a field.</p>
 * <p>This filter is pretty simple and is configured via the 
 * <code>nutchwax.filter.index.setField</code> property.</p>
 * <p>
 * The configuration syntax is a list of white-space delimited key=value
 * pairs of the form:
 * </p><pre>  key=value
 *   key:stored=value
 *   key:stored:tokenized=value</pre>
 * <p>where <code>stored</code> and <code>tokenized</code> are boolean
 * values.  This syntax is similar to that of the
 * <code>ConfigurableIndexingFilter</code> and
 * <code>ConfigurableQueryFilter</code>.</p>
 * <p>The default values of these properties are
 * <code>stored=true</code> and <code>tokenized=false</code>.  The
 * field value <b>is</b> indexed, whether or not it is stored or
 * tokenized.</p>
 * <p>If no value, or an empty value is given in the configuration, then
 * the field is removed from the document all together.</p>
 * <p>This filter is primarily used in situations where the NutchWAX operator
 * makes a mistake and needs to set a field value on all the indexed
 * documents.  For example, if the operator forgets to include the collection
 * name during import, then this filter can be used to set the <code>collection</code>
 * field during indexing.</p>
 */
public class FieldSetter implements IndexingFilter
{
  public static final Log LOG = LogFactory.getLog( FieldSetter.class );

  private Configuration conf;
  private List<FieldSetting> settings = Collections.emptyList();

  public void setConf( Configuration conf )
  {
    this.conf = conf;
    
    String s = conf.get( "nutchwax.filter.index.setField" );
    
    if ( null == s )
      {
        return ;
      }

    s = s.trim( );

    if ( s.length( ) == 0 )
      {
        return ;
      }

    this.settings = new ArrayList<FieldSetting>( );

    // Get field setting, by splitting: "foo=bar frotz=baz" into ["foo=bar","frotz=baz"]
    for ( String field : s.split( "\\s+" ) )
      {
        // Split: "foo:true:false=bar" into ["foo:true:false","bar"]
        String[] fieldParts = field.split("=", 2);

        // Split: "foo:true:false" into ["foo","true","false"]
        String[] keyParts = fieldParts[0].split( "[:]" );

        String  key      = keyParts[0];
        boolean store    = true;
        boolean tokenize = false;
        switch ( keyParts.length )
          {
          default:
            LOG.warn( "Extra fields in field setting ignored: " + fieldParts[0] );
          case 3:
            tokenize = Boolean.parseBoolean( keyParts[2] );
          case 2:
            store    = Boolean.parseBoolean( keyParts[1] );
          case 1:
            // nothing to do
            ;
          }

        String value = fieldParts.length > 1 ? fieldParts[1] : null;

        LOG.info( "Add field setting: " + key + "[" + store + ":" + tokenize + "] = " + value );

        this.settings.add( new FieldSetting( key, store, tokenize, value ) );
      }
  }

  private static class FieldSetting
  {
    String  key;
    boolean store;
    boolean tokenize;
    String  value;

    public FieldSetting( String key, boolean store, boolean tokenize, String value )
    {
      this.key      = key;
      this.store    = store;
      this.tokenize = tokenize;
      this.value    = value;
    }
  }

  public Configuration getConf()
  {
    return this.conf;
  }

  /**
   * <p>Set Lucene document field to fixed value.</p>
   * <p>
   *   Remove field if specified value is <code>null</code>.
   * </p>
   */
  public NutchDocument filter( NutchDocument doc, Parse parse, Text url, CrawlDatum datum, Inlinks inlinks )
    throws IndexingException
  {
    Metadata meta = parse.getData().getContentMeta();

    for ( FieldSetting setting : this.settings )
      {
        // First, remove the existing field.
        doc.removeField( setting.key );

        // Add the value if it is given.
        if ( setting.value != null )
          {
            doc.add( setting.key, setting.value );
          }
      }

    return doc;
  }

  public void addIndexBackendOptions( Configuration conf ) 
  {

    for ( FieldSetting setting : this.settings )
      {
        LuceneWriter.addFieldOptions( setting.key,
                                      setting.store ?    LuceneWriter.STORE.YES : LuceneWriter.STORE.NO,
                                      setting.tokenize ? LuceneWriter.INDEX.TOKENIZED : LuceneWriter.INDEX.UNTOKENIZED, 
                                      conf );
      }

  }
 
}
