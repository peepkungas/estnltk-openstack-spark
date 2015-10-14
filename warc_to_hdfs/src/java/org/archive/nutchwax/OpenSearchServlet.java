/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.archive.nutchwax;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import javax.servlet.ServletException;
import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import javax.xml.parsers.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.w3c.dom.*;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.nutch.searcher.Hit;
import org.apache.nutch.searcher.HitDetails;
import org.apache.nutch.searcher.Hits;
import org.apache.nutch.searcher.NutchBean;
import org.apache.nutch.searcher.Query;
import org.apache.nutch.searcher.Summary;
import org.apache.hadoop.io.FloatWritable;

/** 
 * Present search results using A9's OpenSearch extensions to RSS,
 * plus a few Nutch-specific extensions.
 */   
public class OpenSearchServlet extends HttpServlet 
{
  private static final Map<String,String> NS_MAP = new HashMap<String,String>();
  private int MAX_HITS_PER_PAGE;

  static {
    NS_MAP.put("opensearch", "http://a9.com/-/spec/opensearchrss/1.0/");
    NS_MAP.put("nutch", "http://www.nutch.org/opensearchrss/1.0/");
  }

  private static final Set<String> SKIP_DETAILS = new HashSet<String>();
  static {
    SKIP_DETAILS.add("url");                   // redundant with RSS link
    SKIP_DETAILS.add("title");                 // redundant with RSS title
  }

  private NutchBean bean;
  private Configuration conf;

  public void init(ServletConfig config) throws ServletException {
    try {
      this.conf = NutchConfiguration.get(config.getServletContext());
      bean = NutchBean.get(config.getServletContext(), this.conf);
    } catch (IOException e) {
      throw new ServletException(e);
    }
    MAX_HITS_PER_PAGE = conf.getInt("searcher.max.hits.per.page", -1);
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {

    long responseTime = System.nanoTime( );

    if (NutchBean.LOG.isInfoEnabled()) {
      NutchBean.LOG.info("query request from " + request.getRemoteAddr());
    }

    // get parameters from request
    request.setCharacterEncoding("UTF-8");
    String queryString = request.getParameter("query");
    if (queryString == null) queryString = "";
    //String urlQuery = URLEncoder.encode(queryString, "UTF-8");
    
    // the query language
    String queryLang = request.getParameter("lang");
    
    int start = 0;                                // first hit to display
    String startString = request.getParameter("start");
    if (startString != null)
      start = Integer.parseInt(startString);
    
    int hitsPerPage = 10;                         // number of hits to display
    String hitsString = request.getParameter("hitsPerPage");
    if (hitsString != null)
      hitsPerPage = Integer.parseInt(hitsString);
    if(MAX_HITS_PER_PAGE > 0 && hitsPerPage > MAX_HITS_PER_PAGE)
      hitsPerPage = MAX_HITS_PER_PAGE;

    String sort = request.getParameter("sort");
    boolean reverse = sort != null && "true".equals(request.getParameter("reverse"));

    // De-Duplicate handling.  Look for duplicates field and for how many
    // duplicates per results to return. Default duplicates field is 'site'
    // and duplicates per results default is '2'.
    String dedupField = request.getParameter("dedupField");
    if (dedupField == null || dedupField.length() == 0) {
        dedupField = "site";
    }
    int hitsPerDup = 2;
    String hitsPerDupString = request.getParameter("hitsPerDup");
    String hitsPerSiteString = request.getParameter("hitsPerSite");
    if (hitsPerDupString != null && hitsPerDupString.length() > 0) {
        hitsPerDup = Integer.parseInt(hitsPerDupString);
    } else {
        // If 'hitsPerSite' present, use that value.
        if (hitsPerSiteString != null && hitsPerSiteString.length() > 0) {
            hitsPerDup = Integer.parseInt(hitsPerSiteString);
        }
    }
     
    Query query = Query.parse(queryString, queryLang, this.conf);
    if (NutchBean.LOG.isInfoEnabled()) {
      NutchBean.LOG.info("query: " + queryString);
      NutchBean.LOG.info("lang: " + queryLang);
    }

    // execute the query
    Hits hits;
    try {
      hits = bean.search(query, start + hitsPerPage, hitsPerDup, dedupField, sort, reverse);
    } catch (IOException e) {
      if (NutchBean.LOG.isWarnEnabled()) {
        NutchBean.LOG.warn("Search Error", e);
      }
      hits = new Hits(0,new Hit[0]);	
    }

    if (NutchBean.LOG.isInfoEnabled()) {
      NutchBean.LOG.info("total hits: " + hits.getTotal());
    }

    responseTime = System.nanoTime( ) - responseTime;

    // The 'end' is usually just the end of the current page
    // (start+hitsPerPage); but if we are on the last page
    // of de-duped results, then the end is hits.getLength().
    int end = Math.min( hits.getLength( ), start + hitsPerPage );

    // The length is usually just (end-start), unless the start
    // position is past the end of the results -- which is common when
    // de-duping.  The user could easily jump past the true end of the
    // de-dup'd results.  If the start is past the end, we use a
    // length of '0' to produce an empty results page.
    int length = Math.max( end-start, 0 );

    // Usually, the total results is the total number of non-de-duped
    // results.  Howerver, if we are on last page of de-duped results,
    // then we know our de-dup'd total is hits.getLength().
    long totalResults = hits.getLength( ) < (start+hitsPerPage) ? hits.getLength( ) : hits.getTotal( );

    Hit[]        show      = hits.getHits(start, length );
    HitDetails[] details   = bean.getDetails(show);
    Summary[]    summaries = bean.getSummary(details, query);

    try {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      factory.setNamespaceAware(true);
      Document doc = factory.newDocumentBuilder().newDocument();
 
      Element rss = addNode(doc, doc, "rss");
      addAttribute(doc, rss, "version", "2.0");
      addAttribute(doc, rss, "xmlns:opensearch", NS_MAP.get("opensearch"));
      addAttribute(doc, rss, "xmlns:nutch",      NS_MAP.get("nutch"));

      Element channel = addNode(doc, rss, "channel");
    
      addNode(doc, channel, "title", "Nutch: " + queryString);
      addNode(doc, channel, "description", "Nutch search results for query: " + queryString);
      addNode(doc, channel, "link", "" );

      addNode(doc, channel, "opensearch", "totalResults", ""+totalResults);
      addNode(doc, channel, "opensearch", "startIndex",   ""+start);
      addNode(doc, channel, "opensearch", "itemsPerPage", ""+hitsPerPage);

      addNode(doc, channel, "nutch", "query", queryString);
      addNode(doc, channel, "nutch", "responseTime", Double.toString( ((long) responseTime / 1000 / 1000 ) / 1000.0 ) );

      // Add a <nutch:urlParams> element containing a list of all the URL parameters.
      Element urlParams = doc.createElementNS( NS_MAP.get("nutch"), "urlParams" );
      channel.appendChild( urlParams );

      for ( Map.Entry<String,String[]> e : ((Map<String,String[]>) request.getParameterMap( )).entrySet( ) )
        {
          String key = e.getKey( );
          for ( String value : e.getValue( ) )
            {
              Element urlParam = doc.createElementNS(NS_MAP.get("nutch"), "param" );
              addAttribute( doc, urlParam, "name",  key   );
              addAttribute( doc, urlParam, "value", value );
              urlParams.appendChild(urlParam);
            }
        }

      for (int i = 0; i < length; i++) {
        Hit hit = show[i];
        HitDetails detail = details[i];
        String score = Float.toString( ((FloatWritable)hit.getSortValue( )).get() );
        String title = detail.getValue("title");
        String url   = detail.getValue("url");
      
        if (title == null || title.equals("")) {   // use url for docs w/o title
          title = url;
        }
        
        Element item = addNode(doc, channel, "item");

        addNode(doc, item, "nutch", "score", score );
        addNode(doc, item, "title", title);
        if (summaries[i] != null) {
          addNode(doc, item, "description", summaries[i].toString() );
        }
        addNode(doc, item, "link", url);
        addNode(doc, item, "nutch", "site", hit.getDedupValue());

        for (int j = 0; j < detail.getLength(); j++) { // add all from detail
          String field = detail.getField(j);
          if (!SKIP_DETAILS.contains(field))
            addNode(doc, item, "nutch", field, detail.getValue(j));
        }
      }

      // dump DOM tree

      DOMSource source = new DOMSource(doc);
      TransformerFactory transFactory = TransformerFactory.newInstance();
      Transformer transformer = transFactory.newTransformer();
      transformer.setOutputProperty( javax.xml.transform.OutputKeys.ENCODING, "UTF-8" );
      StreamResult result = new StreamResult(response.getOutputStream());
      response.setContentType("application/rss+xml");
      transformer.transform(source, result);

    } catch (javax.xml.parsers.ParserConfigurationException e) {
      throw new ServletException(e);
    } catch (javax.xml.transform.TransformerException e) {
      throw new ServletException(e);
    }
      
  }

  private static Element addNode(Document doc, Node parent, String name) {
    Element child = doc.createElement(name);
    parent.appendChild(child);
    return child;
  }

  private static void addNode(Document doc, Node parent,
                              String name, String text) {
    if ( text == null ) text = "";
    Element child = doc.createElement(name);
    child.appendChild(doc.createTextNode(getLegalXml(text)));
    parent.appendChild(child);
  }

  private static void addNode(Document doc, Node parent,
                              String ns, String name, String text) {
    if ( text == null ) text = "";
    Element child = doc.createElementNS(NS_MAP.get(ns), ns+":"+name);
    child.appendChild(doc.createTextNode(getLegalXml(text)));
    parent.appendChild(child);
  }

  private static void addAttribute(Document doc, Element node,
                                   String name, String value) {
    Attr attribute = doc.createAttribute(name);
    attribute.setValue(getLegalXml(value));
    node.getAttributes().setNamedItem(attribute);
  }

  /*
   * Ensure string is legal xml.
   * @param text String to verify.
   * @return Passed <code>text</code> or a new string with illegal
   * characters removed if any found in <code>text</code>.
   * @see http://www.w3.org/TR/2000/REC-xml-20001006#NT-Char
   */
  protected static String getLegalXml(final String text) {
      if (text == null) {
          return null;
      }
      StringBuffer buffer = null;
      for (int i = 0; i < text.length(); i++) {
        char c = text.charAt(i);
        if (!isLegalXml(c)) {
	  if (buffer == null) {
              // Start up a buffer.  Copy characters here from now on
              // now we've found at least one bad character in original.
	      buffer = new StringBuffer(text.length());
              buffer.append(text.substring(0, i));
          }
        } else {
           if (buffer != null) {
             buffer.append(c);
           }
        }
      }
      return (buffer != null)? buffer.toString(): text;
  }
 
  private static boolean isLegalXml(final char c) {
    return c == 0x9 || c == 0xa || c == 0xd || (c >= 0x20 && c <= 0xd7ff)
        || (c >= 0xe000 && c <= 0xfffd) || (c >= 0x10000 && c <= 0x10ffff);
  }

}
