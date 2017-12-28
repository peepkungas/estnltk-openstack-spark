Spark task for extracting text from Warc files. Uses Tika to extract text from Warc and then Jsoup to remove HTML tags. 

Usage:
WarcReader <input path> <output path> <additional third param>
where input path is path to Warcfile or directory of warc files,
additional third param determines if output has one directory or several based on file type.

Ignores some Warc records, such as DNS records and warc-fields. Also ignores files that do not provide any meaningful textual data - such as css and javascript files.
