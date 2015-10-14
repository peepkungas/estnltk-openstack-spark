/* 
 * Kaarel T\u00F5nisson 2015.
 */

package org.apache.nutch.indexer.htmldiff;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.nutch.parse.Parse;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.lucene.LuceneWriter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.sksamuel.diffpatch.DiffMatchPatch;
import com.sksamuel.diffpatch.DiffMatchPatch.Diff;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

import org.cyberneko.html.parsers.DOMFragmentParser;
import org.cyberneko.html.parsers.DOMParser;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Node;
import org.w3c.dom.Document;
import org.w3c.dom.html.HTMLDocument;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import org.custommonkey.xmlunit.DetailedDiff;
import org.custommonkey.xmlunit.Difference;
import org.custommonkey.xmlunit.ElementQualifier;
import org.custommonkey.xmlunit.XMLUnit;
import org.custommonkey.xmlunit.examples.RecursiveElementNameAndTextQualifier;
import org.diffxml.diffxml.*;
import org.diffxml.diffxml.xmdiff.Costs;
import org.diffxml.diffxml.xmdiff.XmDiff;

/** 
Finds differences in XML between fetch and last version of fetch. Uses raw html added by HtmlRawParser plugin.
Requires a MongoDB instance to hold last version of fetch for comparison.
The MongoDB collection needs to have composite indexing by url (either ascending or descending) 
and date (descending) to retrieve the newest previous fetch.
 */
public class HtmlDiffIndexer implements IndexingFilter {
	private final Log LOG = LogFactory.getLog(HtmlDiffIndexer.class.getName());
	private Configuration conf;

	private final Logger performanceLog = Logger.getLogger("DiffPerformanceLogger");
	
	private static String MONGODB_IP = "localhost";
	private static short MONGODB_PORT = 27017;
	private static String MONGODB_DBNAME = "nutch";  
	private static String MONGODB_COLLNAME = "lasthtmlbuffer";
	private static String RABBIT_IP = "";
	private static MongoClient mongoClient = null;
	private static DB db = null;
	private static short DIFFEDITCOST = 100;
	private static short MINIMUMDIFFDISTANCE = 3;
	private static String MODELWRITEMODE = "RDF/XML";
	private static boolean OUTPUT_WRITE_DIFFS_TO_SEPARATE_FILES = false;
	private static boolean OUTPUT_WRITE_DIFFS_TO_SINGLE_FILE = false;
	private static boolean DIFF_LOG_NULL_WARNINGS = true;
	private static boolean LOG_PERFORMANCE_TO_FILE = false;
	private static boolean POST_DO_SEND_REQUEST = false;

	
	public NutchDocument filter(NutchDocument doc, Parse parse, 
			Text url, CrawlDatum datum, Inlinks inlinks)
					throws IndexingException {
		// initialise time measurements
		performanceLog.setLevel(Level.INFO);
		Map<String,Long> measurementsMap = new HashMap<String,Long>();
		long startTime = 0;
		long endTime = 0;
			
		// Get date, url and htmlraw from NutchDoc.
		// XXX: NutchWAX gives null datum to document
		//		Date fetchDate = new Date(datum.getFetchTime());
		Date fetchDate = new Date(); 
		String fetchurl = url.toString().split(" ")[0].trim();
		String htmlraw = parse.getData().getParseMeta().get("htmlraw");	

		try{
			measurementsMap.put("doc_size", new Long(htmlraw.length()));
		}catch(Exception e){
			writePerformanceToLog("ERROR_HTML_CONTENT", measurementsMap, fetchurl);
			return doc;
		}
		
		// MongoDB can't handle keys above 1024 bytes (assume trap?)
		if (fetchurl.getBytes().length >= 900) {
			LOG.warn("HtmlRawIndexer: url longer than 900 bytes, avoiding.");
			writePerformanceToLog("URL_TOO_LONG", measurementsMap, fetchurl);
			return doc;
		}

		// Read last fetch from DB.
		startTime = System.currentTimeMillis();
		db.requestEnsureConnection();
		BasicDBObject dbOldDoc = null;
		BasicDBObject query = new BasicDBObject("url",fetchurl);
		DBCollection coll = db.getCollection(MONGODB_COLLNAME);
		dbOldDoc = (BasicDBObject) coll.findOne(query);
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_DBread", new Long(endTime - startTime));

		// Save current fetch to DB.
		startTime = System.currentTimeMillis();
		BasicDBObject dbNewDoc = new BasicDBObject("url",fetchurl).
				append("date",fetchDate).
				append("htmlraw",htmlraw);
		coll.insert(dbNewDoc);

		doc.add("crawldate", fetchDate.toString());
		if(dbOldDoc != null){  // Add last fetch date to NutchDoc. Also checks if last fetch was retrieved from MongoDB.	
			doc.add("lastdate", dbOldDoc.get("date").toString());
		}
		else{ // If couldn't get old doc, can't diff. Return NutchDoc to indexer.
			LOG.info("HtmlRawIndexer: Could not load previous version of " + fetchurl + " from MongoDB. No diff possible.");
			writePerformanceToLog("NO_PREVIOUS_DOC", measurementsMap, fetchurl);
			return doc;
		}
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_DBwrite", new Long(endTime - startTime));


		
		startTime = System.currentTimeMillis();
		LOG.info("URL = "+url);
		LOG.info("htmlraw len = "+htmlraw.length());		

		//--- Parse to xml. ---
		Document newDoc = null;
		Document oldDoc = null;
		try{
			// parse new version HTML to XML
			newDoc = parseHTMLToXMLWithNekoHTML(htmlraw.getBytes("utf-8"));
			// parse old version HTML to XML
			oldDoc = parseHTMLToXMLWithNekoHTML(dbOldDoc.getString("htmlraw").getBytes("utf-8"));

		}catch(Exception e){
			LOG.error("Error parsing HTML to XML : " + e.getMessage());
			writePerformanceToLog("ERROR_XML_PARSING", measurementsMap, fetchurl);
			endTime = System.currentTimeMillis();
			measurementsMap.put("time_parseXML", new Long(endTime - startTime));
			return doc;
		}
		//		org.w3c.dom.Document oldDoc = parser.getDocument();
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_parseXML", new Long(endTime - startTime));
		
		if(newDoc == null || oldDoc == null){
			LOG.error("An XML document is null!");
			writePerformanceToLog("ERROR_XML_NULLDOC", measurementsMap, fetchurl);
			return doc;
		}
		
		measurementsMap.put("oldDoc_nodes", new Long (oldDoc.getElementsByTagName("*").getLength()));
		measurementsMap.put("newDoc_nodes", new Long (newDoc.getElementsByTagName("*").getLength()));
		
		//--- End of parse to xml. ---
		
		
		//--- Diff xmls using xmlunit. ---
		startTime = System.currentTimeMillis();
		List listOfXmlDiffs = null;
		try{
			listOfXmlDiffs = findDiffsWithXMLUnit(newDoc, oldDoc);
		}catch(Exception e){
			LOG.error("HtmlRawIndexer XMLUnit Exception when diffing " + fetchurl +" : " + e.toString());
			writePerformanceToLog("ERROR_DIFFING", measurementsMap, fetchurl);
			endTime = System.currentTimeMillis();
			measurementsMap.put("time_diffXML", new Long(endTime - startTime));
			return doc;
		}
		
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_diffXML", new Long(endTime - startTime));
		
		if(listOfXmlDiffs == null){
			LOG.error("HtmlRawIndexer XMLUnit difflist is null!");
			writePerformanceToLog("NULL_DIFFLIST", measurementsMap, fetchurl);
			return doc;
		}

		if (listOfXmlDiffs.isEmpty()){
			LOG.info("HtmlRawIndexer: No differences found for " + fetchurl);
			writePerformanceToLog("NO_DIFFS_FOUND", measurementsMap, fetchurl);
			return doc;
		}    
		else{
			LOG.info("HtmlRawIndexer: "+ listOfXmlDiffs.size() +" differences detected for " + fetchurl);
			measurementsMap.put("diffcount_diffXML", new Long(listOfXmlDiffs.size()));
		}
		// Debug method: output diffs to log. Not needed for functionality.
		//		writeAllXMLUnitDiffsToLog(listOfXmlDiffs);
		// Debug end. 

	
		startTime = System.currentTimeMillis();
		long oldTimeUnix = ((Date) dbOldDoc.get("date")).getTime() / 1000;
		long fetchTimeUnix = fetchDate.getTime() / 1000;

		Model model = ModelFactory.createDefaultModel();
		
		// --- Create diff Jena model. ---
		try{
			DiffToRdf difftordf = new DiffToRdf(DIFF_LOG_NULL_WARNINGS,DIFFEDITCOST,MINIMUMDIFFDISTANCE);
			model = difftordf.createDiffRdf(listOfXmlDiffs, fetchurl, oldTimeUnix, fetchTimeUnix, dbOldDoc);
		}catch(Exception e){
			// if failed to create model, skip pocessing this page
			LOG.info("Diff RDF creation error: " + e.getMessage());
			writePerformanceToLog("ERROR_JENA_MODEL", measurementsMap, fetchurl);
			return doc;
		}
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_JenaModel", new Long(endTime - startTime));
		
		//--- If enabled, POST diffs to endpoint. ---
		startTime = System.currentTimeMillis();
		if (POST_DO_SEND_REQUEST){
			StringWriter sw = new StringWriter();
			model.write(sw, MODELWRITEMODE);
			createAndSendPOSTRequest(sw.toString()); 
		}
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_DiffsToPOST", new Long(endTime - startTime));

		//--- If enabled, write diffs to file(s). ---
		startTime = System.currentTimeMillis();
		if (OUTPUT_WRITE_DIFFS_TO_SINGLE_FILE | OUTPUT_WRITE_DIFFS_TO_SEPARATE_FILES){
			String filenamePrefix = (fetchDate.toString()  + " " + fetchurl + " ").replaceAll("[:.//]", "_");
			if (OUTPUT_WRITE_DIFFS_TO_SINGLE_FILE){
				try {
					BufferedWriter bw = new BufferedWriter(
							new OutputStreamWriter(
									new FileOutputStream("AllDiffs.txt", true),
									"UTF-8")
							);
					model.write(bw, MODELWRITEMODE);
					bw.close();
					LOG.info("Wrote "+ fetchurl + " to single diff file.");
				} catch (Exception e) {
					LOG.error("HtmlRawIndexer: IOException in writing Diff to file:" + e.getMessage());
					writePerformanceToLog("ERROR_WRITING_TO_SINGLE_FILE", measurementsMap, fetchurl);
					endTime = System.currentTimeMillis();
					measurementsMap.put("time_DiffsToFiles", new Long(endTime - startTime));
					return doc;
				}
			}

			if(OUTPUT_WRITE_DIFFS_TO_SEPARATE_FILES){
				try {
					PrintWriter out = new PrintWriter(filenamePrefix+"JenaOutput.txt");
					model.write(out, MODELWRITEMODE);
					out.close();
					LOG.info("Wrote "+ fetchurl + " to separate diff file.");
				} catch (FileNotFoundException e) {
					LOG.error("HtmlRawIndexer: Unable to write Diff model to file:" + e.toString());
					writePerformanceToLog("ERROR_WRITING_TO_SEPARATE_FILE", measurementsMap, fetchurl);
					endTime = System.currentTimeMillis();
					measurementsMap.put("time_DiffsToFiles", new Long(endTime - startTime));
					return doc;
				}
			}
		}
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_DiffsToFiles", new Long(endTime - startTime));
		//--- End of diff to file output. ---

		//--- Performance comparison : Diff xmls using diffxml xmdiff. ---
		startTime = System.currentTimeMillis();
		int smallestXmDiffDist = -1;
		try{
			XMDiff xmdiff = new XMDiff();
			smallestXmDiffDist = xmdiff.performXMdiff(oldDoc,newDoc, fetchurl);
		}catch(Exception e){
			LOG.error("Error performing xmdiff : " + e.getMessage());
			writePerformanceToLog("ERROR_XMDIFF", measurementsMap, fetchurl);
			endTime = System.currentTimeMillis();
			measurementsMap.put("time_xmdiff", new Long(endTime - startTime));
			return doc;
		}
		endTime = System.currentTimeMillis();
		measurementsMap.put("time_xmdiff", new Long(endTime - startTime));
		measurementsMap.put("dist_xmdiff", new Long(smallestXmDiffDist));
		
		writePerformanceToLog("SUCCESS", measurementsMap, fetchurl);
		
		return doc;
	}

	/**
	 * Parses HTML into XML using the NekoHTML parser.
	 * @param bs Byte array of the HTML code of page.
	 * @return xmlDoc XML document created from the provided HTML.
	 * @throws UnsupportedEncodingException
	 * @throws SAXException
	 * @throws IOException
	 */
	
	private Document parseHTMLToXMLWithNekoHTML(byte[] bs)
			throws UnsupportedEncodingException, SAXException, IOException {
//		parser.setProperty("http://cyberneko.org/html/properties/default-encoding", "UTF-8");
		DOMParser parser = new DOMParser();
		parser.setFeature("http://xml.org/sax/features/namespaces", false);
		InputSource htmlSource;
		Document xmlDoc;
		htmlSource = new InputSource(new ByteArrayInputStream(bs));
		parser.parse(htmlSource);
		xmlDoc = parser.getDocument();
		return xmlDoc;
	}

	/**
	 * Find Diffs of two Documents. XMLUnit diffing is used in this implementation.
	 * @param oldDoc one Document
	 * @param newDoc another Document
	 * @return List of Diffs of the documents
	 */
	private List findDiffsWithXMLUnit(Document newDoc, Document oldDoc) {
		List listOfXmlDiffs = null;
		org.custommonkey.xmlunit.Diff xmlDiff = new org.custommonkey.xmlunit.Diff(oldDoc, newDoc);
		LOG.info("xmlunit diff identical = " + xmlDiff.identical() + ", similar = " + xmlDiff.similar());
		org.custommonkey.xmlunit.DetailedDiff detailedDiff = new DetailedDiff(xmlDiff);
		detailedDiff.overrideElementQualifier(new org.custommonkey.xmlunit.examples.RecursiveElementNameAndTextQualifier());
		listOfXmlDiffs = detailedDiff.getAllDifferences();
		return listOfXmlDiffs;
	}


	/**
	 * Outputs to log file the time spent at each step of the process.
	 * @param measurementsMap Map<String, Long> containing measurement name and duration pairs.
	 * @param fetchurl String of URL of the page being processed. 
	 */
	private void writePerformanceToLog(String message, Map<String, Long> measurementsMap, String fetchurl){
		if (LOG_PERFORMANCE_TO_FILE == true){
			try {
				String line = 
						message + "\t" + 
						fetchurl + "\t" + 		
						"doc_size=" + measurementsMap.get("doc_size") + "\t" +
						"oldDoc_nodes=" + measurementsMap.get("oldDoc_nodes") + "\t" +
						"newDoc_nodes=" + measurementsMap.get("newDoc_nodes") + "\t" +
						"time_DBread=" + measurementsMap.get("time_DBread") + "\t" + 
						"time_DBwrite=" + measurementsMap.get("time_DBwrite") + "\t" +
						"time_parseXML=" + measurementsMap.get("time_parseXML") + "\t" + 
						"time_diffXML=" + measurementsMap.get("time_diffXML") + "\t" +
						"diffcount_diffXML=" + measurementsMap.get("diffcount_diffXML") + "\t" +
						"time_JenaModel=" + measurementsMap.get("time_JenaModel") + "\t" +
						"time_xmdiff=" + measurementsMap.get("time_xmdiff") + "\t" + 
						"dist_xmdiff=" + measurementsMap.get("dist_xmdiff") + "\t" + 
						"time_DiffsToPOST=" + measurementsMap.get("time_DiffsToPOST") + "\t" + 
						"time_DiffsToFiles=" + measurementsMap.get("time_DiffsToFiles");
				performanceLog.info(line);
			} catch (Exception e) {
				LOG.info("Failed to save performance to file :" + e.getMessage());
			}
		}
	}

	/**
	 * Convenience method for assigning values from Nutch configuration to local variables.
	 * @param localVariable name of local String variable
	 * @param paramName name of parameter in nutch-site.xml
	 * @param conf Nutch Configuration object
	 * @return value of parameter. If null, localVariable.
	 */
	private String assignValuesFromConfig(String localVariable, String paramName, Configuration conf){
		try{
			if (conf.get(paramName) != null){
				return conf.get(paramName);
			}else{
				LOG.warn("No value for "+ paramName +" found in config. Using default value " + localVariable);
				return localVariable;
			}
		}
		catch (Exception e){
			LOG.warn("Error finding value for "+ paramName +" in config. Using default value " + localVariable);
			return localVariable;
		}
	}

	/**
	 * Convenience method for assigning values from Nutch configuration to local variables.
	 * @param localVariable name of local short variable
	 * @param paramName name of parameter in nutch-site.xml
	 * @param conf Nutch Configuration object
	 * @return value of parameter. If null, localVariable.
	 */
	private short assignValuesFromConfig(short localVariable, String paramName, Configuration conf){
		try{
			if (conf.get(paramName) != null){
				return Short.parseShort(conf.get(paramName));
			}else{
				LOG.warn("No value for "+ paramName +" found in config. Using default value " + localVariable);
				return localVariable;
			}
		}
		catch (Exception e){
			LOG.warn("Error finding value for "+ paramName +" in config. Using default value " + localVariable);
			return localVariable;
		}
	}

	/**
	 * Convenience method for assigning values from Nutch configuration to local variables.
	 * @param localVariable name of local boolean variable
	 * @param paramName name of parameter in nutch-site.xml
	 * @param conf Nutch Configuration object
	 * @return value of parameter. If not "true" or "false" in config, localVariable.
	 */
	private boolean assignValuesFromConfig(boolean localVariable, String paramName, Configuration conf){	
		try{
			if (conf.get(paramName).equals("true")){
				return true;
			}
			else if (conf.get(paramName).equals("false")){
				return false;
			}
			else{
				LOG.warn("No value for "+ paramName +" found in config. Using default value " + localVariable);
				return localVariable;
			}
		}
		catch (Exception e){
			LOG.warn("Error finding value for "+ paramName +" in config. Using default value " + localVariable);
			return localVariable;
		}
	}
	
	/** Creates HTML POST request and sends it to pubsub server.
	 * 
	 * @param diffsString found diffs in string form (assumed following http://vocab.deri.ie/diff ontology), without error checking
	 */
	private void createAndSendPOSTRequest(String diffsString) {
		try {	  
			String urlParameters = "diffs=" + diffsString;
			String urlString = RABBIT_IP;
			URL serverUrlObj;
			serverUrlObj = new URL("http://"+urlString);
			HttpURLConnection connection = (HttpURLConnection) serverUrlObj.openConnection();           
			connection.setDoOutput(true);
			//		  connection.setDoInput(true);
			//		  connection.setInstanceFollowRedirects(false); 
			connection.setRequestMethod("POST"); 
			//		  connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded"); 
			connection.setRequestProperty("charset", "utf-8");
			//		  connection.setRequestProperty("Content-Length", "" + Integer.toString(urlParameters.getBytes().length));
			//		  connection.setUseCaches (false);

			DataOutputStream wr = new DataOutputStream( connection.getOutputStream() );
			wr.writeBytes(urlParameters);
			wr.flush();
			wr.close();
			int responseCode = connection.getResponseCode();
			LOG.info("DIFF POST response code: " + responseCode);
			connection.disconnect(); 
		} catch (MalformedURLException e) {
			LOG.error("Malformed URL exception in htmlrawindexer POST : " + e.getMessage());
		} catch (IOException e) {
			LOG.error("IOException in htmlrawindexer POST : " + e.getMessage());
		}
	}

	/**
	 * Debugging method for writing all XMLUnit diffs to console.
	 * @param listOfXmlDiffs
	 */
	private void writeAllXMLUnitDiffsToLog(List<Difference> listOfXmlDiffs) {
		for (Difference thisDifference : listOfXmlDiffs){
			try{
				LOG.info("Difference ID: " + thisDifference.getId());
				LOG.info("Difference description value: " + thisDifference.getDescription());
				LOG.info("Control node nodename: " + thisDifference.getControlNodeDetail().getNode().getNodeName());				
				LOG.info("Test node nodename: " + thisDifference.getTestNodeDetail().getNode().getNodeName());
				if(thisDifference.getControlNodeDetail().getValue() != null){
					LOG.info("Control node value: " + thisDifference.getControlNodeDetail().getValue());
				}
				if(thisDifference.getTestNodeDetail().getValue() != null){
					LOG.info("Test node value: " + thisDifference.getTestNodeDetail().getValue());
				}
				LOG.info("Control node XPATH: " + thisDifference.getControlNodeDetail().getXpathLocation());
				LOG.info("Test node XPATH: " + thisDifference.getTestNodeDetail().getXpathLocation());
			}catch(Exception e){
				LOG.info("Failed to write to log :" + e.getMessage());
			}
		}
	}

	/**
	 * Sets values based on config. Called from outside, in filter application. 
	 */
	public void setConf(Configuration conf) {
		// Retrieve configuration values.
		Configuration nutchConf = NutchConfiguration.create();
		MONGODB_IP = assignValuesFromConfig(MONGODB_IP,"htmlrawindexer.mongodb.ip",nutchConf);
		MONGODB_PORT = assignValuesFromConfig(MONGODB_PORT,"htmlrawindexer.mongodb.port",nutchConf);
		MONGODB_DBNAME = assignValuesFromConfig(MONGODB_DBNAME,"htmlrawindexer.mongodb.dbname",nutchConf);
		MONGODB_COLLNAME = assignValuesFromConfig(MONGODB_COLLNAME,"htmlrawindexer.mongodb.collectionname",nutchConf);
		RABBIT_IP = assignValuesFromConfig(RABBIT_IP,"htmlrawindexer.hub.ip",nutchConf);
		DIFFEDITCOST = assignValuesFromConfig(DIFFEDITCOST,"htmlrawindexer.diffmatchpatch.editcost",nutchConf);
		MINIMUMDIFFDISTANCE = assignValuesFromConfig(MINIMUMDIFFDISTANCE,"htmlrawindexer.diffmatchpatch.minimumdistance",nutchConf);
		MODELWRITEMODE = assignValuesFromConfig(MODELWRITEMODE,"mongodb.jena.writemode",nutchConf);
		OUTPUT_WRITE_DIFFS_TO_SEPARATE_FILES = assignValuesFromConfig(OUTPUT_WRITE_DIFFS_TO_SEPARATE_FILES,"htmlrawindexer.output.writediffstoseparatefiles",nutchConf);
		OUTPUT_WRITE_DIFFS_TO_SINGLE_FILE = assignValuesFromConfig(OUTPUT_WRITE_DIFFS_TO_SINGLE_FILE,"htmlrawindexer.output.writediffstosinglefile",nutchConf);
		POST_DO_SEND_REQUEST = assignValuesFromConfig(POST_DO_SEND_REQUEST,"htmlrawindexer.post.dopostrequest",nutchConf);
		DIFF_LOG_NULL_WARNINGS = assignValuesFromConfig(DIFF_LOG_NULL_WARNINGS,"htmlrawindexer.debug.logdiffnulls",nutchConf);
		LOG_PERFORMANCE_TO_FILE = assignValuesFromConfig(LOG_PERFORMANCE_TO_FILE,"htmlrawindexer.performance.logperformancetofile",nutchConf);
		String logPerformanceToFilenameString = "performance_measurements.log";
		logPerformanceToFilenameString = assignValuesFromConfig(logPerformanceToFilenameString,"htmlrawindexer.performance.logperformancefilename",nutchConf);
		
		// Check MongoDB connection. Error out if unable to.  
		if (db == null || db.getName() != MONGODB_DBNAME ){
			try {
				mongoClient = new MongoClient(MONGODB_IP, MONGODB_PORT);
				db = mongoClient.getDB(MONGODB_DBNAME);
			} catch (UnknownHostException e1) {
				LOG.error("Could not connect to MongoDB (Unknown Host Exception)");
			}
		}

		// Performance log file handler : if output file is already handled, don't add again.

		try {
			Handler[] handlerArray = performanceLog.getHandlers();
			boolean hasFileHandler = false;
			for (Handler oneHandler:handlerArray){
				if (oneHandler.getClass().equals(FileHandler.class)){
					hasFileHandler = true;
					break;
				};
			}
			if (hasFileHandler == false){
				FileHandler fh = new FileHandler(logPerformanceToFilenameString, true);
				fh.setFormatter(new SimpleFormatter());
				performanceLog.addHandler(fh);
				performanceLog.setUseParentHandlers(false);
			}
		} catch (Exception e) {
			LOG.error("Error confing file handler for performance logger : " + e.getMessage());

		}
		
		this.conf = conf;
	}

	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void addIndexBackendOptions(Configuration conf) {
		LuceneWriter.addFieldOptions("crawldate", LuceneWriter.STORE.YES,LuceneWriter.INDEX.NO, conf);
		LuceneWriter.addFieldOptions("lastdate", LuceneWriter.STORE.YES,LuceneWriter.INDEX.NO, conf);

	}

}
