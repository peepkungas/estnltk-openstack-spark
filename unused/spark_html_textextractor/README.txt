Spark task for parsing SequenceFiles containing key-value pairs (with value=HTML of a page) into key-value pairs (preserving key from the SequenceFile). Uses Boilerpipe for parsing.

usage: 
SparkHtmlTextExtractor <path>
where path is either a SequenceFile or a folder containing SequenceFiles.

TODO:
	* add piping support (currently simply saves result to file)