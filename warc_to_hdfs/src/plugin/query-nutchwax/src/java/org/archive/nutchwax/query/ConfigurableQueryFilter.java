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
package org.archive.nutchwax.query;

import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.index.Term;

import org.apache.nutch.searcher.QueryFilter;
import org.apache.nutch.searcher.QueryException;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Query.Clause;
import org.apache.nutch.searcher.FieldQueryFilter;
import org.apache.nutch.searcher.RawFieldQueryFilter;
import org.apache.hadoop.conf.Configuration;

/** 
 * 
 */
public class ConfigurableQueryFilter implements QueryFilter
{
  private List<QueryFilter> filters;
  private Configuration     conf;

  public ConfigurableQueryFilter( )
  {
    this.filters = new ArrayList<QueryFilter>( );
  }

  public void setConf( Configuration conf )
  {
    this.conf = conf;
    
    this.constructFilters( );
  }


  public Configuration getConf( )
  {
    return this.conf;
  }


  public BooleanQuery filter( Query input, BooleanQuery output )
    throws QueryException 
  {
    for ( QueryFilter filter : this.filters )
      {
        output = filter.filter(input, output);
      }
    
    return output;
  }

  private void constructFilters( )
  {
    String filterSpecs = conf.get( "nutchwax.filter.query" );
    
    if ( null == filterSpecs )
      {
        return ;
      }

    filterSpecs = filterSpecs.trim( );

    for ( String filterSpec : filterSpecs.split("\\s+") )
      {
        String spec[] = filterSpec.split("[:]");
        
        if ( spec.length < 2 )
          {
            // TODO: Warning
            continue;
          }

        if ( "field".equals( spec[0] ) )
          {
            String name  = spec[1];
            float  boost = 1.0f;
            if ( spec.length > 2 )
              {
                try
                  {
                    boost = Float.parseFloat( spec[2] );
                  }
                catch ( NumberFormatException nfe )
                  {
                    // TODO: Warning, but ignore it.
                  }
              }
            QueryFilter filter = new FieldQueryFilterImpl( name, boost );

            this.filters.add( filter );
          }
        else if ( "raw".equals( spec[0] ) )
          {
            String  name      = spec[1];
            boolean lowerCase = true;
            float   boost     = 1.0f;
            if ( spec.length > 2 )
              {
                lowerCase = Boolean.parseBoolean( spec[2] );
              }
            if ( spec.length > 3 )
              {
                try
                  {
                    boost = Float.parseFloat( spec[2] );
                  }
                catch ( NumberFormatException nfe )
                  {
                    // TODO: Warning, but ignore it.
                  }
              }

            QueryFilter filter = new RawFieldQueryFilterImpl( name, lowerCase, boost );
            
            this.filters.add( filter );
          }
        else if ( "group".equals( spec[0] ) )
          {
            String  name      = spec[1];
            boolean lowerCase = true;
            String  delimiter = ",";
            float   boost     = 1.0f;
            if ( spec.length > 2 )
              {
                lowerCase = Boolean.parseBoolean( spec[2] );
              }
            if ( spec.length > 3 )
              {
                delimiter = spec[3];
              }
            if ( spec.length > 4 )
              {
                try
                  {
                    boost = Float.parseFloat( spec[4] );
                  }
                catch ( NumberFormatException nfe )
                  {
                    // TODO: Warning, but ignore it.
                  }
              }
            QueryFilter filter = new GroupedQueryFilter( name, delimiter, lowerCase, boost );
            
            this.filters.add( filter );
          }
        else
          {
            // TODO: Warning uknown filter type
          }
      }
  }

  public class RawFieldQueryFilterImpl extends RawFieldQueryFilter
  {
    private Configuration conf;

    public RawFieldQueryFilterImpl( String field, boolean lowerCase, float boost )
    {
      super( field, lowerCase, boost );

      // Use the same conf as the owning instance.
      this.setConf( ConfigurableQueryFilter.this.conf );
    }

    public Configuration getConf( )
    {
      return this.conf;
    }
    
    public void setConf( Configuration conf )
    {
      this.conf = conf;
    }
  }

  public class FieldQueryFilterImpl extends FieldQueryFilter
  {
    public FieldQueryFilterImpl( String field, float boost )
    {
      super( field, boost );

      // Use the same conf as the owning instance.
      this.setConf( ConfigurableQueryFilter.this.conf );
    }
  }

  public class GroupedQueryFilter implements QueryFilter
  {
    private String  field;
    private String  delimiter;
    private boolean lowerCase;
    private float   boost;
    private Configuration conf;
    
    /** Construct for the named field, potentially lowercasing query values.*/
    public GroupedQueryFilter( String field, String delimiter, boolean lowerCase, float boost )
    {
      this.field     = field;
      this.delimiter = delimiter;
      this.lowerCase = lowerCase;
      this.boost     = boost;

      // Use the same conf as the owning instance.
      this.setConf( ConfigurableQueryFilter.this.conf );
    }
    
    public BooleanQuery filter( Query input, BooleanQuery output )
      throws QueryException 
    {
      // examine each clause in the Nutch query
      for ( Clause c : input.getClauses() )
        {
          // skip non-matching clauses
          if ( !c.getField( ).equals( field ) ) continue;
          
          // get the field value from the clause
          // raw fields are guaranteed to be Terms, not Phrases
          String values = c.getTerm().toString();

          BooleanQuery group = new BooleanQuery( output.isCoordDisabled( ) );
          for ( String value : values.split( this.delimiter ) )
            {
              if (lowerCase) value = value.toLowerCase();

              // Create a Lucene TermQuery for this value
              TermQuery term = new TermQuery( new Term( field, value ) );

              term.setBoost(boost);

              // Add it to the group
              group.add( term, BooleanClause.Occur.SHOULD );
            }

          // Finally add the group to the overall query.  The group's
          // must/not/should is taken from the original Nutch clause
          // with the multiple values.
          output.add( group, (c.isProhibited()
                              ? BooleanClause.Occur.MUST_NOT
                              : (c.isRequired()
                                 ? BooleanClause.Occur.MUST
                                 : BooleanClause.Occur.SHOULD
                                 )
                              ));
        }
      
      // return the modified Lucene query
      return output;
    }

    public void setConf( Configuration conf )
    {
      this.conf = conf;
    }
    
    public Configuration getConf( )
    {
      return this.conf;
    }
  }

}
