/* Slightly altered code, originally taken from the diffxml package by Adrian Mouat
 * 
 * This implementation by Kaarel T\u00F5nisson 2015.
 */
package org.apache.nutch.indexer.htmldiff;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.diffxml.diffxml.xmdiff.Costs;
import org.diffxml.diffxml.xmdiff.XmDiff;
import org.w3c.dom.Document;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

public class XMDiff {
	private final Log LOG = LogFactory.getLog(HtmlDiffIndexer.class.getName());

	/**
	 * Performs xmdiff on two documents. Creates temporary files for that as xmdiff uses external memory.
	 * @param oldDoc one Document
	 * @param newDoc another Document
	 * @param uri TODO
	 * @throws XmlPullParserException 
	 */
	public int performXMdiff(Document oldDoc, Document newDoc, String uri) 
			throws TransformerConfigurationException, TransformerFactoryConfigurationError, 
			IOException, TransformerException, XmlPullParserException {

		XmDiff xmdiff = new XmDiff();
		
		XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
		factory.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);

		XmlPullParser doc1 = factory.newPullParser();
		XmlPullParser doc2 = factory.newPullParser();

		File newDocTempFile = outputXMLToTempFile(newDoc, "newDoc");
		File oldDocTempFile = outputXMLToTempFile(oldDoc, "oldDoc");
		newDocTempFile.deleteOnExit();
		oldDocTempFile.deleteOnExit();
		String f1 = newDocTempFile.getAbsolutePath();
		String f2 = oldDocTempFile.getAbsolutePath();
		
		doc1.setInput ( new FileReader ( f1 ) );
		doc2.setInput ( new FileReader ( f2 ) );
		
//		LOG.info("XMDIFF DEBUG CHECK A");
		//Intermediate temp files
		//Create output file
		File tmp1 = File.createTempFile("xdiff",null,null);
		File tmp2 = File.createTempFile("xdiff",null,null);
//		File out = File.createTempFile("xdiff",null,null);
		tmp1.deleteOnExit();
		tmp2.deleteOnExit();
		RandomAccessFile fA = new RandomAccessFile(tmp1, "rw");
		RandomAccessFile fB = new RandomAccessFile(tmp2, "rw");

//		LOG.info("XMDIFF DEBUG CHECK B");
		//Algorithm mmdiff
		int D[][] = new int[8000][8000];	
		D[0][0]=0;
//		LOG.info("XMDIFF DEBUG CHECK C");
		//Calculate delete costs
		//Returns number of nodes in doc1
	    int num_doc1=delcosts(doc1, D, fA);
//	    LOG.info("XMDIFF DEBUG CHECK D");
		//Calculate insert costs
		//Returns number of nodes in doc2
		int num_doc2=inscosts(doc2, D, fB);

//		LOG.info("XMDIFF DEBUG CHECK E");
		//Calculate everything else
		//Need to reset inputs
		//doc1.setInput ( new FileReader ( args [0] ) );
	        //doc2.setInput ( new FileReader ( args [1] ) );
		//Need to be able to reset parser so pass filename with parser
		xmdiff.allcosts(doc1, f1, num_doc1, doc2, f2, num_doc2, D);
		
		int smallestLength = D[num_doc1-1][num_doc2-1];
//		LOG.info("XMDIFF DEBUG CHECK F");
		newDocTempFile.delete();
		oldDocTempFile.delete();
//		LOG.info("XMDIFF DEBUG CHECK FIN");
		return smallestLength;
	}
	
	/**
	 * Outputs XML of Document to a temporary file. Returns the temp file handle.
	 * @param filename name of output file. ".tmp" suffix is appended automatically.
	 * @param Document doc Document object containing XML.
	 * @return temporary File handle.
	 */
	private File outputXMLToTempFile(Document xmlDoc, String filename)
			throws TransformerFactoryConfigurationError,
			TransformerConfigurationException, IOException,
			TransformerException {
		TransformerFactory transformerFactory = TransformerFactory.newInstance();
		Transformer transformer = transformerFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
		DOMSource source = new DOMSource(xmlDoc);
		File tempFile = File.createTempFile(filename, ".tmp");
		FileOutputStream fos = new FileOutputStream(tempFile, false);
		OutputStreamWriter osw = new OutputStreamWriter(fos, "UTF-8");
		StreamResult result = new StreamResult(osw);
		transformer.transform(source, result);
		
		
//		//XXX: DEBUG
//		File debug_f = new File(filename+"debug");
//		FileOutputStream debug_fos = new FileOutputStream(debug_f, false);
//		OutputStreamWriter debug_osw = new OutputStreamWriter(debug_fos, "UTF-8");
//		StreamResult debugresult = new StreamResult(debug_osw);
//		transformer.transform(source, debugresult);
//		debug_fos.close();
//		//END of debug
		
		return tempFile;
	}
	
	
    public int delcosts(XmlPullParser doc1, int[][] D, RandomAccessFile fA)
            throws XmlPullParserException, IOException
        {
    	//Currently cost of inserting and deleting is fixed
    	//So this effectively does little more than count nodes
    	//But different if costd or costi depend on node

    	//Also prints out XPaths

    	int i=1;
            int eventType = doc1.getEventType();
            ArrayList path = new ArrayList();
    	int depth=0; //Easier & quicker to keep our own depth setting
    	int tmp;


    	//Need to init first entry
    	
    	//path.add( new Integer(0) );

    	//Skip first (hopefully root) tag
    	if(eventType == XmlPullParser.START_TAG) 
    		{
    		doc1.next();
    		}
    		

            do {
                if(eventType == XmlPullParser.START_TAG) {

    		//Deletion costs
    		D[i][0]=D[(i-1)][0] + Costs.costDelete(); //+ costd(node);
    		i++;

    		//Path 
    		if ( depth < doc1.getDepth() )
    			{
    			path.add( new Integer(1) );
    			depth ++;
    			}
    		else if (depth > doc1.getDepth() )
    			{
    			path.remove((depth-1));
    			depth--;
    			tmp = ( (Integer) path.get((depth-1))).intValue();
    			path.set( (depth-1), (new Integer(++tmp)) );
    			}
    		else 
    			{//depth == doc1.getDepth()
    			tmp = ( (Integer) path.get( (depth-1) )).intValue();
                            path.set( (depth-1), (new Integer(++tmp)) );
    			}
    			
    		

    		//Output Path
    		
                } else if(eventType == XmlPullParser.TEXT) {

    		D[i][0]=D[(i-1)][0] + Costs.costDelete(); // + costd(node);
    		i++;
    		
    		//Do Path Stuff
    		//We actually want to consider text nodes at depth getDepth +1

    		int txt_depth=doc1.getDepth()+1;
    		if ( depth < txt_depth )
                            {
                            path.add( (new Integer(1)) );
                            depth ++;
                            }
                    else if (depth > txt_depth )
                            {
                            path.remove((txt_depth-1));
                            depth--;
                            tmp = ( (Integer) path.get( (depth-1) )).intValue();
                            path.set( (depth-1), (new Integer(++tmp)) );
                            }
                    else
                            {//depth == doc1.getDepth()
                            tmp = ( (Integer) path.get( (depth-1) )).intValue();
                            path.set( (depth-1), (new Integer(++tmp)) );
                            }
    		//System.out.println("Depth=" + depth);
    	    }
    	   
                eventType = doc1.next();
            } while (eventType != XmlPullParser.END_DOCUMENT);

    	//System.out.println("Number of nodes: " + (i-1));
    	return((i));
        }
    public int inscosts(XmlPullParser doc2, int[][] D, RandomAccessFile fB)
    		throws XmlPullParserException, IOException
    		{
    		int j=1;
    		int eventType = doc2.getEventType();
    		ArrayList path = new ArrayList();
    	        int depth=0; //Easier & quicker to keep our own depth setting
    	        int tmp;


    		//Skip first (hopefully root) tag
    	        if(eventType == XmlPullParser.START_TAG)
    	       		{
    	                doc2.next();
    	                }

    		do {
    	            if(eventType == XmlPullParser.START_TAG) 
    			{
    	                D[0][j]=D[0][(j-1)] + Costs.costInsert(); //+ costi(node);
    	                j++;

    			//Path
    	                if ( depth < doc2.getDepth() )
    	                        {
    	                        path.add( new Integer(1) );
    	                        depth ++;
    	                        }
    	                else if (depth > doc2.getDepth() )
    	                        {
    	                        path.remove((depth-1));
    	                        depth--;
    	                        tmp = ( (Integer) path.get((depth-1))).intValue();
    	                        path.set( (depth-1), (new Integer(++tmp)) );
    	                        }
    	                else
    	                        {//depth == doc1.getDepth()
    	                        tmp = ( (Integer) path.get( (depth-1) )).intValue();
    	                        path.set( (depth-1), (new Integer(++tmp)) );
    	                        }

    			}
    	            else if(eventType == XmlPullParser.TEXT) 
    			{
    	                D[0][j]=D[0][(j-1)] + Costs.costInsert(); // + costi(node);
    	                j++;

    			//Do Path Stuff
    	                //We actually want to consider text nodes at depth getDepth +1
    	 
    	                int txt_depth=doc2.getDepth()+1;
    	                if ( depth < txt_depth )
    	                        {
    	                        path.add( (new Integer(1)) );
    	                        depth ++;
    	                        }
    	                else if (depth > txt_depth )
    	                        {
    	                        path.remove((txt_depth-1));
    	                        depth--;
    	                        tmp = ( (Integer) path.get( (depth-1) )).intValue();
    	                        path.set( (depth-1), (new Integer(++tmp)) );
    	                        }
    	                else
    	                        {//depth == doc1.getDepth()
    	                        tmp = ( (Integer) path.get( (depth-1) )).intValue();
    	                        path.set( (depth-1), (new Integer(++tmp)) );
    	                        }


    	            	}
    	            eventType = doc2.next();
    	           } while (eventType != XmlPullParser.END_DOCUMENT);

    	        //System.out.println("Number of nodes: " + (j-1));
    		return((j));
    		}


}
