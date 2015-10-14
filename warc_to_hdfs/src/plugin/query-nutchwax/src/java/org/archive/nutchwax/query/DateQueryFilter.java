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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.TermQuery;

import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Query.Clause;
import org.apache.nutch.searcher.QueryException;
import org.apache.nutch.searcher.QueryFilter;

/** 
 * <p>
 *   Filter on a date or date range.  This filter assumes the dates
 *   are in field named "date" and adhere to the IA 14-digit date
 *   format: YYYYMMDDHHMMSS
 * </p>
 * <p>
 *   Date values in the query can have less than the full 14-digit
 *   precision.  In that case, they are converted into a range over
 *   given precision.  For example, "date:2007" is automagically
 *   converted into "date[20070000000000-20079999999999]".
 * </p>
 * <p>
 *   NOTE: In order for this filter to take advantage of the Nutch
 *   auto-magic conversion of RangeQuery into RangeFilter, we have to
 *   create the RangeQuery with:
 *   <ul>
 *   <li>occur = BooleanClause.Occur.MUST</li>
 *   <li>boost = 0.0f;</li>
 *   </ul>
 *   These are the two conditions that Nutch's LuceneQueryOptimizer
 *   checks before doing a RangeQuery-&gt;RangeFilter conversion.
 * </p>
 */
public class DateQueryFilter implements QueryFilter
{
  // public static final Log LOG = LogFactory.getLog( DateQueryFilter.class );
  
  private static final String  FIELD = "date";
  private static final Pattern queryPattern = Pattern.compile("^(\\d{4,14})$|^(\\d{4,14})(?:-(\\d{4,14}))$");

  private Configuration conf;
  
  public void setConf( Configuration conf )
  {
    this.conf = conf;
  }
  
  public Configuration getConf()
  {
    return this.conf;
  }

  public BooleanQuery filter( Query input, BooleanQuery output )
    throws QueryException
  {
    Clause [] clauses = input.getClauses();
    
    for ( Clause clause : clauses )
      {
        // Skip non-date clauses
        if ( ! FIELD.equals( clause.getField() ) )
          {
            continue ;
          }

        Matcher matcher = queryPattern.matcher( clause.getTerm().toString() );

        if ( matcher == null || !matcher.matches() )
          {
            // TODO: Emit message
            throw new QueryException( "" );
          }

        String date = matcher.group( 1 );

        if ( date != null )
          {
            doDateQuery( output, clause, date );

            continue ;
          }

        String lower = matcher.group( 2 );
        String upper = matcher.group( 3 );
        
        if ( lower != null && upper != null )
          {
            doRangeQuery( output, clause, lower, upper );

            continue ;
          }

        // No matching groups -- weird since the matcher.matches()
        // check succeeded above.
        // TODO: Emit message
        throw new QueryException( "" );
      }

    return output;
  }

  private void doDateQuery( BooleanQuery output, Clause clause, String date )
  {
    // If the date has less than the 14-digit precision, make it into a range
    // query for the precision given.  E.g., if the date is "20080510", then
    // make it into a "[20080510000000-20080510999999]"
    if ( date.length() < 14 )
      {
        String lower = this.padDate( date, '0' );
        String upper = this.padDate( date, '9' );

        doRangeQuery( output, clause, lower, upper );

        return ;
      }

    // Otherwise make it plain-old TermQuery to match the exact date.
    TermQuery term = new TermQuery( new Term( FIELD, date ) );
    
    // Not strictly required since this is a TermQuery and not a
    // RangeQuery, but we use the same 0.0f boost for consistency.
    term.setBoost( 0.0f );

    output.add( term, BooleanClause.Occur.MUST );
  }

  private void doRangeQuery( BooleanQuery output, Clause clause, String lower, String upper )
  {
    lower = padDate( lower );
    upper = padDate( upper );

    RangeQuery range = new RangeQuery( new Term( FIELD, lower ),
                                       new Term( FIELD, upper ),
                                       true );

    // Required for LuceneQueryOptimizer to convert to RangeFilter.
    range.setBoost( 0.0f );

    output.add( range, BooleanClause.Occur.MUST );
  }


  private String padDate( final String date, final char pad )
  {
    StringBuilder buf = new StringBuilder( date );
    int len = 14 - date.length( );
    for ( int i = 0; i < len ; i++ )
      {
        buf.append( pad );
      }

    return buf.toString( );
  }

  private String padDate( String date )
  {
    return padDate( date, '0' );
  }

}
