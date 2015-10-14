<?xml version="1.0" encoding="utf-8" ?> 
<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->
<xsl:stylesheet
     version="1.0"
     xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
     xmlns:nutch="http://www.nutch.org/opensearchrss/1.0/"
     xmlns:opensearch="http://a9.com/-/spec/opensearchrss/1.0/" 
>
<xsl:output method="xml" />

<xsl:template match="rss/channel">
  <html xmlns="http://www.w3.org/1999/xhtml">
  <head>
  <title><xsl:value-of select="title" /></title>
  <style media="all" lang="en" type="text/css">
  body
  {
    padding     : 20px;
    margin      : 0;
    font-family : Verdana; sans-serif;
    font-size   : 9pt;
    color : #000000;
    background-color: #ffffff;
  }
  .pageTitle
  {
    font-size   : 125% ;
    font-weight : bold ;
    text-align  : center ;
    padding-bottom : 2em ;
  }
  .searchForm
  {
    margin : 20px 0 5px 0;
    padding-bottom : 0px;
    border-bottom : 1px solid black;
  }
  .searchResult
  {
    margin  : 0;
    padding : 0;
  }
  .searchResult h1 
  {
    margin  : 0 0 5px 0 ;
    padding : 0 ;
    font-size : 120%;
  }
  .searchResult .details
  {
    font-size: 80%;
    color: green;
  }
  .searchResult .dates
  {
    font-size: 80%;
  }
  .searchResult .dates a
  {
    color: #3366cc;
  }
  form#searchForm
  {
    margin : 0; padding: 0 0 10px 0;
  }
  .searchFields
  {
    padding : 3px 0;
  }
  .searchFields input
  {
    margin  : 0 0 0 15px;
    padding : 0;
  }
  input#query
  {
    margin : 0;
  }
  ol
  {
    margin  : 5px 0 0 0;
    padding : 0 0 0 2em;
  }
  ol li
  {
    margin : 0 0 15px 0;
  }
  </style>
  </head>
  <body>
    <!-- Page header: title and search form -->
    <div class="pageTitle" >
      NutchWAX Sample XSLT
    </div>
    <div>
      This simple XSLT demonstrates the transformation of OpenSearch XML results into a fully-functional, human-friendly HTML search page.  No JSP needed.
    </div>
    <div class="searchForm">
      <form id="searchForm" name="searchForm" method="get" action="search" >
        <span class="searchFields">
        Search for 
        <input id="query" name="query" type="text" size="40" value="{nutch:query}" />

        <!-- Create hidden form fields for the rest of the URL parameters -->
        <xsl:for-each select="nutch:urlParams/nutch:param[@name!='start' and @name!='query']">
          <xsl:element name="input" namespace="http://www.w3.org/1999/xhtml">
            <xsl:attribute name="type">hidden</xsl:attribute>
            <xsl:attribute name="name" ><xsl:value-of select="@name"  /></xsl:attribute>
            <xsl:attribute name="value"><xsl:value-of select="@value" /></xsl:attribute>
          </xsl:element>
        </xsl:for-each>

        <input type="submit" value="Search"/>
        </span>
      </form>
    </div>
    <div style="font-size: 8pt; margin:0; padding:0 0 0.5em 0;">Results <xsl:value-of select="opensearch:startIndex + 1" />-<xsl:value-of select="opensearch:startIndex + opensearch:itemsPerPage" /> of about <xsl:value-of select="opensearch:totalResults" /> <span style="margin-left: 1em;"></span></div>
    <!-- Search results -->
    <ol start="{opensearch:startIndex + 1}">
      <xsl:apply-templates select="item" />
    </ol>
    <!-- Generate list of page links -->
    <center>
      <xsl:call-template name="pageLinks">
        <xsl:with-param name="labelPrevious" select="'&#171;'" />
        <xsl:with-param name="labelNext"     select="'&#187;'" />
      </xsl:call-template>
    </center>
  </body>
</html>
</xsl:template>


<!-- ======================================================================
     NutchWAX XSLT template/fuction library.
     
     The idea is that the above xhtml code is what most NutchWAX users
     will modify to tailor to their own look and feel.  The stuff
     below implements the core logic for generating results lists,
     page links, etc.

     Hopefully NutchWAX web developers will be able to easily edit the
     above xhtml and css and won't have to change the below.
     ====================================================================== -->

<!-- Template to emit a search result as an HTML list item (<li/>).
  -->
<xsl:template match="item">
  <li>
  <div class="searchResult">
    <h1><a href="{concat('http://wayback.archive-it.org/',nutch:collection,'/',nutch:date,'/',link)}"><xsl:value-of select="title" /></a></h1>
    <div>
      <xsl:value-of select="description" />
    </div>
    <div class="details">
      <xsl:value-of select="link" /> - <xsl:value-of select="round( nutch:length div 1024 )"/>k - <xsl:value-of select="nutch:type" />
    </div>
    <div class="dates">
      <a href="{concat('http://wayback.archive-it.org/',nutch:collection,'/*/',link)}">All versions</a> - <a href="?query={../nutch:query} site:{nutch:site}&amp;hitsPerSite=0">More from <xsl:value-of select="nutch:site" /></a>
    </div>
  </div>
  </li>
</xsl:template>

<!-- Template to emit a date in YYYY/MM/DD format 
  -->
<xsl:template match="nutch:date" >
  <xsl:value-of select="substring(.,1,4)" /><xsl:text>-</xsl:text><xsl:value-of select="substring(.,5,2)" /><xsl:text>-</xsl:text><xsl:value-of select="substring(.,7,2)" /><xsl:text> </xsl:text>
</xsl:template>

<!-- Template to emit a list of numbered page links, *including*
     "previous" and "next" links on either end, using the given labels.
     Parameters:
       labelPrevious   Link text for "previous page" link
       labelNext       Link text for "next page" link
  -->
<xsl:template name="pageLinks">
  <xsl:param name="labelPrevious" />
  <xsl:param name="labelNext"     />
  <xsl:variable name="startPage" select="floor(opensearch:startIndex   div opensearch:itemsPerPage) + 1" />
  <xsl:variable name="lastPage"  select="floor(opensearch:totalResults div opensearch:itemsPerPage) + 1" />
  <!-- If we are on any page past the first, emit a "previous" link -->
  <xsl:if test="$startPage != 1">
    <xsl:call-template name="pageLink">
      <xsl:with-param name="pageNum"  select="$startPage - 1" />
      <xsl:with-param name="linkText" select="$labelPrevious" />
    </xsl:call-template>
    <xsl:text> </xsl:text>
  </xsl:if>
  <!-- Now, emit numbered page links -->
  <xsl:choose>
    <!-- We are on pages 1-10.  Emit links  -->
    <xsl:when test="$startPage &lt; 11">
      <xsl:choose>
        <xsl:when test="$lastPage &lt; 21">
          <xsl:call-template name="numberedPageLinks" >
            <xsl:with-param name="begin"   select="1"  />
            <xsl:with-param name="end"     select="$lastPage + 1" />
            <xsl:with-param name="current" select="$startPage" />
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="numberedPageLinks" >
            <xsl:with-param name="begin"   select="1"  />
            <xsl:with-param name="end"     select="21" />
            <xsl:with-param name="current" select="$startPage" />
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <!-- We are past page 10, but not to the last page yet.  Emit links for 10 pages before and 10 pages after -->
    <xsl:when test="$startPage &lt; $lastPage">
      <xsl:choose>
        <xsl:when test="$lastPage &lt; ($startPage + 11)">
          <xsl:call-template name="numberedPageLinks" >
            <xsl:with-param name="begin"   select="$startPage - 10" />
            <xsl:with-param name="end"     select="$lastPage  +  1" />
            <xsl:with-param name="current" select="$startPage"      />
          </xsl:call-template>
        </xsl:when>
        <xsl:otherwise>
          <xsl:call-template name="numberedPageLinks" >
            <xsl:with-param name="begin"   select="$startPage - 10" />
            <xsl:with-param name="end"     select="$startPage + 11" />
            <xsl:with-param name="current" select="$startPage"      />
          </xsl:call-template>
        </xsl:otherwise>
      </xsl:choose>
    </xsl:when>
    <!-- This covers the case where we are on (or past) the last page -->
    <xsl:otherwise>
      <xsl:call-template name="numberedPageLinks" >
        <xsl:with-param name="begin"   select="$startPage - 10" />
        <xsl:with-param name="end"     select="$lastPage  + 1"  />
        <xsl:with-param name="current" select="$startPage"      />
      </xsl:call-template>
    </xsl:otherwise>
  </xsl:choose>
  <!-- Lastly, emit a "next" link. -->
  <xsl:text> </xsl:text>
  <xsl:if test="$startPage &lt; $lastPage">
    <xsl:call-template name="pageLink">
      <xsl:with-param name="pageNum"  select="$startPage + 1" />
      <xsl:with-param name="linkText" select="$labelNext" />
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<!-- Template to emit a list of numbered links to results pages. 
     Parameters:
       begin    starting # inclusive
       end      ending # exclusive
       current  the current page, don't emit a link
  -->
<xsl:template name="numberedPageLinks">
  <xsl:param name="begin"   />
  <xsl:param name="end"     />
  <xsl:param name="current" />
  <xsl:if test="$begin &lt; $end">
    <xsl:choose>
      <xsl:when test="$begin = $current" >
        <xsl:value-of select="$current" />
      </xsl:when>
      <xsl:otherwise>
        <xsl:call-template name="pageLink" >
          <xsl:with-param name="pageNum"  select="$begin"  />
          <xsl:with-param name="linkText" select="$begin"  />
        </xsl:call-template>
      </xsl:otherwise>
    </xsl:choose>
    <xsl:text> </xsl:text>
    <xsl:call-template name="numberedPageLinks">
      <xsl:with-param name="begin"   select="$begin + 1" />
      <xsl:with-param name="end"     select="$end"       />
      <xsl:with-param name="current" select="$current"   />
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<!-- Template to emit a single page link.  All of the URL parameters
     listed in the OpenSearch results are included in the link.
     Parmeters:
       pageNum    page number of the link
       linkText   text of the link
  -->
<xsl:template name="pageLink">
  <xsl:param name="pageNum"  />
  <xsl:param name="linkText" />
  <xsl:element name="a" namespace="http://www.w3.org/1999/xhtml">
    <xsl:attribute name="href">
      <xsl:text>?</xsl:text>
      <xsl:for-each select="nutch:urlParams/nutch:param[@name!='start']">
        <xsl:value-of select="@name" /><xsl:text>=</xsl:text><xsl:value-of select="@value" />
        <xsl:text>&amp;</xsl:text>
      </xsl:for-each>
      <xsl:text>start=</xsl:text><xsl:value-of select="($pageNum -1) * opensearch:itemsPerPage" />
    </xsl:attribute>
    <xsl:value-of select="$linkText" />
  </xsl:element>
</xsl:template>

</xsl:stylesheet>
