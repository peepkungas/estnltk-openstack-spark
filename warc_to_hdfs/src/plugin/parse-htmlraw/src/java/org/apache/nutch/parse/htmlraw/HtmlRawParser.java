/* 
 * Kaarel T\u00F5nisson 2015.
 */

package org.apache.nutch.parse.htmlraw;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.protocol.Content;
import org.w3c.dom.DocumentFragment;


/** 
 * Parse raw HTML into metatag of document.
 ***/

public class HtmlRawParser implements HtmlParseFilter {

  private static final Log LOG = LogFactory.getLog(HtmlRawParser.class.getName());
  
  private Configuration conf;

  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  public ParseResult filter(Content content, ParseResult parseResult, 
		  HTMLMetaTags metaTags, DocumentFragment doc) {
	 	  
	Parse parse = parseResult.get(content.getUrl());
	Metadata metadata = parse.getData().getParseMeta();
	
	byte[] contentInOctets = content.getContent();
	String htmlraw = new String();
	try {
		//XXX: utf-8 only? could get encoding from parseresult?
		htmlraw = new String (contentInOctets,"UTF-8");
	} catch (UnsupportedEncodingException e) {
		LOG.error("unable to convert content into string");
	}

	metadata.add("htmlraw", htmlraw);
    LOG.info("Added parse meta tag: \"htmlraw\", length="+htmlraw.length());
    
    return parseResult;
  }

}
