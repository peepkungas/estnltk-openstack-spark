WARC to SequenceFile converter
by Kaarel Tõnisson

This program converts WARC archive files into Hadoop SequenceFiles for cluster usage. The program is built on NutchWAX, with a replaced Importer.  

Usage: Call ImporterToHdfs, simplified by calling
	bin/nutchwax import_to_hdfs <manifestfile>
where manifestfile is a file containing a path to a WARC file on each row.

Output: SequenceFiles are written into folder sequencefiles, with each file containing content from one WARC. Non-text contect is discarded. Sequencefiles contain key-value pairs where:
	key - domain::path::date
	value - unprocessed HTML of the page
SequenceFiles are in a compressed format and therefore are not human-readable.

Relevant nutch-site.xml configuration parameters:
nutchwax.importer.hdfs.seqfileprefix : Prefix string to be added to output files
nutchwax.importer.hdfs.seqfilesuffix : Suffix string to be added to output files
nutchwax.importer.hdfs.seqfilepath : Location where sequencefiles are output to 

Build command (Javadoc errors, but succeeds):
ant package