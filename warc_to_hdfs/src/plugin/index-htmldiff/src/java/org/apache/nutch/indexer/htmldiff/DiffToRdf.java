/* 
 * Kaarel T\u00F5nisson 2015.
 */

package org.apache.nutch.indexer.htmldiff;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.custommonkey.xmlunit.Difference;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.mongodb.BasicDBObject;
import com.sksamuel.diffpatch.DiffMatchPatch;
import com.sksamuel.diffpatch.DiffMatchPatch.Diff;

public class DiffToRdf {
	private final Log LOG = LogFactory.getLog(HtmlDiffIndexer.class.getName());	
	private boolean DIFF_LOG_NULL_WARNINGS;
	private short DIFFEDITCOST;
	private int MINIMUMDIFFDISTANCE;

	public DiffToRdf(boolean diff_log_null_warnings, short diff_edit_cost,
			short minimum_diff_distance) {
		this.DIFF_LOG_NULL_WARNINGS = diff_log_null_warnings;
		this.DIFFEDITCOST = diff_edit_cost;
		this.MINIMUMDIFFDISTANCE = minimum_diff_distance;
	}

	/** Creates Jena model of changeset.
	 * 
	 * @param listOfXMLDiffs List of XMLUnit differences.
	 * @param fetchurl URL of the diffed page
	 * @param oldUnixTime the UNIX time of the previous fetch of this page
	 * @param fetchUnixTime the UNIX time of this fetch of this page
	 * @param dbOldDoc MongoDB database object of the previous fetch of this page
	 * @return Jena Model object of the created RDF.
	 */
	public Model createDiffRdf (List<Difference> listOfXMLDiffs, String fetchurl, long oldUnixTime, long fetchUnixTime, BasicDBObject dbOldDoc){  
		// make sure that fetch url ends with /
		if( fetchurl.charAt( fetchurl.length() -1 ) != '/'){
			fetchurl.concat("/");
		}

		String urlWithDiffAndTimes = fetchurl + "diff/" + oldUnixTime + "-" + fetchUnixTime;
		Model model = ModelFactory.createDefaultModel();

		/*	      
	      #Use diff ontology available at: http://vocab.deri.ie/diff by using the N-Triples syntax described at: http://www.w3.org/TR/n-triples/ .
		  #Namespace: http://vocab.deri.ie/diff#

		  #To construct identifiers for diffs (sets of changes between documents), add suffix "/diff/<timestamp1>-<timestamp2>" to URLs of monitored Web pages, where timestamp1 and timestamp2 are correspondingly the numbers of seconds since Unix epoch (also called Unix time) identifying timestamp of retrieving the previous and current version of a Web page. Thus the identifier of diff of page http://www.postimees.ee retrievad at Unix times 1396187504 and 1396187534 will be http://www.postimees.ee/diff/1396187504-1396187534.

		  <http://www.postimees.ee/diff/1396187504-1396187534> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#Diff> .# State that http://www.postimees.ee/diff/1396187504-1396187534 is a diff instance
		  <http://www.postimees.ee/diff/1396187504-1396187534> <http://vocab.deri.ie/diff#objectOfChange> <http://www.postimees.ee/1396187504> .# Property used to link the "Diff" object to the next version of the document created by this diff. http://www.postimees.ee/1396187504 is the identifier of http://www.postimees.ee Web page at time 1396187504.
		  <http://www.postimees.ee/diff/1396187504-1396187534> <http://vocab.deri.ie/diff#subjectOfChange> <http://www.postimees.ee/1396187534> .# Property used to link the "Diff" object to the previous version of the document changed by this diff.
		 */

		Resource urlWithDiffAndTimesResource = 
				model.createResource(urlWithDiffAndTimes)
				.addProperty(
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
						"http://vocab.deri.ie/diff#Diff"
						)
						.addProperty(
								model.createProperty("http://vocab.deri.ie/diff#objectOfChange"), 
								fetchurl + oldUnixTime
								)
								.addProperty(
										model.createProperty("http://vocab.deri.ie/diff#subjectOfChange"), 
										fetchurl + fetchUnixTime
										);

		int nrOfInsertsAndDeletes = 0;
		for (Difference thisDiff:listOfXMLDiffs){
			/*
			 #To construct identifiers for individual diff components add suffix "/N" to diff identifier, whereas N will be the sequence number of a diff component. State explicitly diff components for reference deletion and insertion, sentence deletion and insertion.

			 <http://www.postimees.ee/diff/1396187504-1396187534/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#ReferenceDeletion> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/2> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#ReferenceInsertion> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#SentenceDeletion> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/4> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#SentenceInsertion> .

			 #Use http://purl.org/dc/terms/hasPart relation to bind components of diffs to a particular diff object:

			 <http://www.postimees.ee/diff/1396187504-1396187534> <http://purl.org/dc/terms/hasPart> <http://www.postimees.ee/diff/1396187504-1396187534/1> .
		 	 <http://www.postimees.ee/diff/1396187504-1396187534> <http://purl.org/dc/terms/hasPart> <http://www.postimees.ee/diff/1396187504-1396187534/2> .
			 <http://www.postimees.ee/diff/1396187504-1396187534> <http://purl.org/dc/terms/hasPart> <http://www.postimees.ee/diff/1396187504-1396187534/3> .
			 <http://www.postimees.ee/diff/1396187504-1396187534> <http://purl.org/dc/terms/hasPart> <http://www.postimees.ee/diff/1396187504-1396187534/4> .    

		     #Removed and added fragments are represented as text objects. To identify these objects use suffixes "/addition" and "/removal". For instance added and removed fragments of diff component http://www.postimees.ee/diff/1396187504-1396187534/1 will be respectively http://www.postimees.ee/diff/1396187504-1396187534/1/addition and http://www.postimees.ee/diff/1396187504-1396187534/1/removal.

		     <http://www.postimees.ee/diff/1396187504-1396187534/1> <http://vocab.deri.ie/diff#removal> <http://www.postimees.ee/diff/1396187504-1396187534/1/removal> .
		     <http://www.postimees.ee/diff/1396187504-1396187534/2> <http://vocab.deri.ie/diff#addition> <http://www.postimees.ee/diff/1396187504-1396187534/2/addition> .
		     <http://www.postimees.ee/diff/1396187504-1396187534/3> <http://vocab.deri.ie/diff#removal> <http://www.postimees.ee/diff/1396187504-1396187534/3/removal> .
		     <http://www.postimees.ee/diff/1396187504-1396187534/4> <http://vocab.deri.ie/diff#addition> <http://www.postimees.ee/diff/1396187504-1396187534/4/addition> .

			 #Identify that these objects are text blocks

		     <http://www.postimees.ee/diff/1396187504-1396187534/1/removal> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#TextBlock> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/2/addition> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#TextBlock> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/3/removal> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#TextBlock> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/4/addition> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://vocab.deri.ie/diff#TextBlock> .

			 #Determine content of these text blocks

			 <http://www.postimees.ee/diff/1396187504-1396187534/1/removal> <http://vocab.deri.ie/diff#content> "http://tallinncity.postimees.ee/2740048/juri-ennet-savisaar-on-eesti-vabariigi-aiaisand-ja-erakondliku-sumfooniaorkestri-dirigent" .
			 <http://www.postimees.ee/diff/1396187504-1396187534/2/addition> <http://vocab.deri.ie/diff#content> "http://e24.postimees.ee/2737324/arengufondi-rahastamisotsus-sundis-hamaratel-asjaoludel" .
			 <http://www.postimees.ee/diff/1396187504-1396187534/3/removal> <http://vocab.deri.ie/diff#content> "Riigil ja rahval" .
			 <http://www.postimees.ee/diff/1396187504-1396187534/4/addition> <http://vocab.deri.ie/diff#content> "Kooliharidus" .

			 #Determine location (line number) of these text blocks

			 <http://www.postimees.ee/diff/1396187504-1396187534/1/removal> <http://vocab.deri.ie/diff#lineNumber> "112"^^<http://www.w3.org/2001/XMLSchema#integer> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/2/addition> <http://vocab.deri.ie/diff#lineNumber> "111"^^<http://www.w3.org/2001/XMLSchema#integer> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/3/removal> <http://vocab.deri.ie/diff#lineNumber> "4"^^<http://www.w3.org/2001/XMLSchema#integer> .
			 <http://www.postimees.ee/diff/1396187504-1396187534/4/addition> <http://vocab.deri.ie/diff#lineNumber> "4"^^<http://www.w3.org/2001/XMLSchema#integer> .
			 */



			String newFragmentContent;
			String oldFragmentContent;	
			try{
				newFragmentContent = thisDiff.getTestNodeDetail().getValue();
			}catch(Exception e){
				newFragmentContent = "";	// null content to blank string
			}
			try{
				oldFragmentContent = thisDiff.getControlNodeDetail().getValue();
			}catch(Exception e){
				oldFragmentContent = "";	// null content to blank string
			}

			//--- if reference/link ---
			
			//XXX: Some nodenames are null, therefore aren't legal links
			String nodeName = "";
			try{
				nodeName = thisDiff.getControlNodeDetail().getNode().getNodeName();
			}
			catch (NullPointerException e){
				if (DIFF_LOG_NULL_WARNINGS){
					LOG.warn("Diff to Jena: getNodeName() returns null");
				}
			}
			if (nodeName.compareTo("href") == 0 || 
					nodeName.compareTo( "src") == 0){
				nrOfInsertsAndDeletes += 1;
				Resource urlWithDiffSequenceNumberResource =
						model.createResource(urlWithDiffAndTimes + "/" + nrOfInsertsAndDeletes);
				urlWithDiffAndTimesResource.addProperty(			// Identify this diff as part of changeset
						model.createProperty("http://purl.org/dc/terms/hasPart"),
						urlWithDiffSequenceNumberResource
						);			
				// create addition fragment
				urlWithDiffSequenceNumberResource.addProperty(		// Identify diff as ReferenceInsertion
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
						"http://vocab.deri.ie/diff#ReferenceInsertion"
						);			 
				Resource urlWithDiffSequenceAndAdditionFragmentResource = model.createResource(urlWithDiffSequenceNumberResource + "/addition");
				urlWithDiffSequenceNumberResource.addProperty(		// Identify fragment as addition
						model.createProperty("http://vocab.deri.ie/diff#addition"), 
						urlWithDiffSequenceAndAdditionFragmentResource
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Identify addition fragment content as text
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						"http://vocab.deri.ie/diff#TextBlock"
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Determine addition fragment content
						model.createProperty("http://vocab.deri.ie/diff#content"),
						newFragmentContent
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Determine addition fragment location XXX:Currently XPATH (not in vocabulary) 
						model.createProperty("http://vocab.deri.ie/diff#lineNumber"),
						thisDiff.getTestNodeDetail().getXpathLocation()
						);

				// create removal fragment
				nrOfInsertsAndDeletes += 1;
				urlWithDiffSequenceNumberResource =
						model.createResource(urlWithDiffAndTimes + "/" + nrOfInsertsAndDeletes);


				urlWithDiffAndTimesResource.addProperty(				// Identify this diff as part of changeset
						model.createProperty("http://purl.org/dc/terms/hasPart"),
						urlWithDiffSequenceNumberResource
						);
				urlWithDiffSequenceNumberResource.addProperty(		// Identify diff as ReferenceDeletion
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
						"http://vocab.deri.ie/diff#ReferenceDeletion"
						);
				Resource urlWithDiffSequenceAndRemovalFragmentResource = model.createResource(urlWithDiffSequenceNumberResource + "/removal");
				urlWithDiffSequenceNumberResource.addProperty(		// Identify fragment as removal
						model.createProperty("http://vocab.deri.ie/diff#removal"), 
						urlWithDiffSequenceAndRemovalFragmentResource
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Identify removal fragment content as text
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						"http://vocab.deri.ie/diff#TextBlock"
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Determine removal fragment content
						model.createProperty("http://vocab.deri.ie/diff#content"),
						oldFragmentContent
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Determine removal fragment location XXX:Currently XPATH (not in vocabulary)
						model.createProperty("http://vocab.deri.ie/diff#lineNumber"),
						thisDiff.getControlNodeDetail().getXpathLocation()
						);

			}
			//--- if text ---
			else if (thisDiff.getDescription().compareTo("text value") == 0){
				// ignore minor differences
				LinkedList <Diff> textDiffs = new LinkedList<Diff>();
				DiffMatchPatch diffMatchPatchObj = new DiffMatchPatch();
				diffMatchPatchObj.Diff_EditCost = DIFFEDITCOST;
				diffMatchPatchObj.Diff_Timeout = 0;
				textDiffs = diffMatchPatchObj.diff_main(
						oldFragmentContent,
						newFragmentContent
						);
				if (diffMatchPatchObj.diff_levenshtein(textDiffs) < MINIMUMDIFFDISTANCE){
					continue;
				}

				nrOfInsertsAndDeletes += 1;
				Resource urlWithDiffSequenceNumberResource =
						model.createResource(urlWithDiffAndTimes + "/" + nrOfInsertsAndDeletes);
				urlWithDiffAndTimesResource.addProperty(			// Identify this diff as part of changeset
						model.createProperty("http://purl.org/dc/terms/hasPart"),
						urlWithDiffSequenceNumberResource
						);
				// create addition fragment
				urlWithDiffSequenceNumberResource.addProperty(		// Identify diff as SentenceInsertion
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
						"http://vocab.deri.ie/diff#SentenceInsertion"
						);
				Resource urlWithDiffSequenceAndAdditionFragmentResource = model.createResource(urlWithDiffSequenceNumberResource + "/addition");
				urlWithDiffSequenceNumberResource.addProperty(		// Identify fragment as addition
						model.createProperty("http://vocab.deri.ie/diff#addition"), 
						urlWithDiffSequenceAndAdditionFragmentResource
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Identify addition fragment content as text
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						"http://vocab.deri.ie/diff#TextBlock"
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Determine addition fragment content
						model.createProperty("http://vocab.deri.ie/diff#content"),
						newFragmentContent
						);
				urlWithDiffSequenceAndAdditionFragmentResource.addProperty(   // Determine addition fragment location XXX:Currently XPATH (not in vocabulary) 
						model.createProperty("http://vocab.deri.ie/diff#lineNumber"),
						thisDiff.getTestNodeDetail().getXpathLocation()
						);
				// create removal fragment
				nrOfInsertsAndDeletes += 1;
				urlWithDiffSequenceNumberResource =
						model.createResource(urlWithDiffAndTimes + "/" + nrOfInsertsAndDeletes);
				urlWithDiffAndTimesResource.addProperty(				// Identify this diff as part of changeset
						model.createProperty("http://purl.org/dc/terms/hasPart"),
						urlWithDiffSequenceNumberResource
						);
				urlWithDiffSequenceNumberResource.addProperty(		// Identify diff as SentenceDeletion
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
						"http://vocab.deri.ie/diff#SentenceDeletion"
						);
				Resource urlWithDiffSequenceAndRemovalFragmentResource = model.createResource(urlWithDiffSequenceNumberResource + "/removal");
				urlWithDiffSequenceNumberResource.addProperty(		// Identify fragment as removal
						model.createProperty("http://vocab.deri.ie/diff#removal"), 
						urlWithDiffSequenceAndRemovalFragmentResource
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Identify removal fragment content as text
						model.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
						"http://vocab.deri.ie/diff#TextBlock"
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Determine removal fragment content
						model.createProperty("http://vocab.deri.ie/diff#content"),
						oldFragmentContent
						);
				urlWithDiffSequenceAndRemovalFragmentResource.addProperty(   // Determine removal fragment location XXX:Currently XPATH (not in vocabulary)
						model.createProperty("http://vocab.deri.ie/diff#lineNumber"),
						thisDiff.getControlNodeDetail().getXpathLocation()
						);
			}

		} // end loop	

		return model;
	} // end createDiffRdf

}
