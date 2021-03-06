This program converts WARC archive files into Hadoop SequenceFiles for cluster usage. The program is built on NutchWAX, with a replaced Importer.  

Build command (may give Javadoc errors, but still succeed):
    ant package

Usage: Call ImporterToHdfs, easiest by calling
    bin/nutchwax import_to_hdfs <manifestfile>
where manifestfile is a file containing a path to a WARC file on each row (Exact file paths only. Wildcards and directories are not accepted).

Output: SequenceFiles are written into the directory specified in nutch-site.xml, with each file containing content from one WARC. Non-text content (metadata) is discarded. Sequencefiles contain key-value pairs in the format:
    key - domain::path::date
    value - HTML of the page
SequenceFiles are in a compressed format and therefore are not human-readable.

Relevant conf/nutch-site.xml configuration parameters. Edit them accoridng to your needs:
    nutchwax.importer.hdfs.seqfileprefix : Prefix string to be added to output files
    nutchwax.importer.hdfs.seqfilesuffix : Suffix string to be added to output files
    nutchwax.importer.hdfs.seqfilepath : Output directory (location of created sequencefiles). For hdfs use full path hdfs://<hdfs_server>:<port>/<location_of_created_sequencefiles>
    nutchwax.importer.hdfs.manifestPath : Output directory for manifest.txt file (warc files to be processed)
    nutchwax.importer.hdfs.processedPath : Output directory for .processed file (all processed warc files will be added)

Example:
Let's have three WARC files located in "/home/kaarelt/warcfiles". We want to create sequencefiles from them and output them into "/home/kaarelt/seqfiles".
1) In  conf/nutch-site.xml, find and edit the value of the property:
    property : nutchwax.importer.hdfs.seqfilepath
    value : /home/kaarelt/seqfiles
2) Create the file "/home/kaarelt/manifests/manifest.txt" containing the paths to each of the WARC files:
    /home/kaarelt/warcfiles/warc1.warc
    /home/kaarelt/warcfiles/warc2.warc
    /home/kaarelt/warcfiles/warc3.warc
3) Call the converter on the manifest file:
    bin/nutchwax import_to_hdfs /home/kaarelt/manifests/manifest.txt
This will output one sequencefile for each WARC input file into "/home/kaarelt/seqfiles".


For automatic start (manifest.txt file is created automatically) use cron to call warc to hdfs shell script (scripts/warc_to_hdfs_execution.sh)
    -f=<nutchwax_location>
    -p=<warc_files_location>
    -l=<script_output_log_file_localtion>

Example:
$ crontab -l
$ 0 * * * * /bin/sh /home/kaarelt/estnltk-openstack-spark/warc_to_hdfs/scripts/warc_to_hdfs_execution.sh -f=/home/kaarelt/estnltk-openstack-spark/warc_to_hdfs/bin/nutchwax -p=/home/kaarelt/warcfiles -l=/home/kaarelt/logs/warc_to_hdfs.log

