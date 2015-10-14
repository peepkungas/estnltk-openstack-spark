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
package org.archive.nutchwax;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.transform.Templates;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.stream.StreamResult;


public class CacheSettingsFilter implements Filter
{
  private String maxAge;

  public void init( FilterConfig config )
    throws ServletException
  {
    this.maxAge = config.getInitParameter( "max-age" );

    if ( this.maxAge != null )
      {
        this.maxAge = this.maxAge.trim( );
        
        if ( this.maxAge.length( ) == 0 )
          {
            this.maxAge = null;
          }
        else
          {
            this.maxAge = "max-age=" + this.maxAge;
          }
      }
  }

  public void doFilter( ServletRequest request, ServletResponse response, FilterChain chain )
    throws IOException, ServletException 
  {
    HttpServletResponse res = (HttpServletResponse) response;
    
    res.setDateHeader( "Date", System.currentTimeMillis( ) );

    if ( this.maxAge != null )
      {
        res.addHeader( "Cache-Control", this.maxAge );
      }

    chain.doFilter( request, res );
  }

  public void destroy()
  {

  }

}
