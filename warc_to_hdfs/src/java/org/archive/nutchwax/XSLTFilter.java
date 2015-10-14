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


public class XSLTFilter implements Filter
{
  private String xsltUrl;
  private String contentType;

  public void init( FilterConfig config )
    throws ServletException
  {
    this.xsltUrl = config.getInitParameter( "xsltUrl" );

    if ( this.xsltUrl != null )
      {
        this.xsltUrl = this.xsltUrl.trim( );
        
        if ( this.xsltUrl.length( ) == 0 )
          {
            this.xsltUrl = null;
          }
      }

    this.contentType = config.getInitParameter( "contentType" );

    if ( this.contentType != null )
      {
        this.contentType = this.contentType.trim( );
        
        if ( this.contentType.length( ) == 0 )
          {
            this.contentType = null;
          }
      }

    if ( this.contentType == null )
      {
        this.contentType = "application/xml";
      }
  }

  public void doFilter( ServletRequest request, ServletResponse response, FilterChain chain )
    throws IOException, ServletException 
  {
    if ( this.xsltUrl != null )
      {
        ByteArrayOutputStream baos = new ByteArrayOutputStream( 8 * 1024 );

        HttpServletResponseInterceptor capturedResponse = new HttpServletResponseInterceptor( (HttpServletResponse) response, baos );
        
        chain.doFilter( request, capturedResponse );
        
        byte output[] = baos.toByteArray( );
        
        try
          {
            Source      xsltSource    = new StreamSource( xsltUrl );
            Templates   xsltTemplates = TransformerFactory.newInstance( ).newTemplates( xsltSource );
            Transformer transformer   = xsltTemplates.newTransformer( );
            
            StreamSource source = new StreamSource( new ByteArrayInputStream( output ) );
            StreamResult result = new StreamResult( response.getOutputStream( ) );
            
            // Enforce XML content-type in the response.
            response.setContentType( this.contentType );
            
            transformer.transform( source, result );
          }
        catch ( javax.xml.transform.TransformerConfigurationException tce )
          {
            // TODO: Re-throw, or log it and eat it?
          }
        catch( javax.xml.transform.TransformerException te )
          {
            // TODO: Re-throw, or log it and eat it?
          }
      }
    else
      {
        chain.doFilter( request, response );
      }
  }

  public void destroy()
  {

  }

}


class HttpServletResponseInterceptor extends HttpServletResponseWrapper
{
  private OutputStream os;

  HttpServletResponseInterceptor( HttpServletResponse response, OutputStream os )
  {
    super( response );
    
    this.os = os;
  }

  public ServletOutputStream getOutputStream() 
  {
    ServletOutputStream sos = new ServletOutputStream( )
      {
        public void write( int b )
          throws java.io.IOException
        {
          HttpServletResponseInterceptor.this.os.write( b );
        }
      };
    
    return sos;
  }

  public PrintWriter getWriter( )
  {
    PrintWriter pw = new PrintWriter( this.os );

    return pw;
  }

}