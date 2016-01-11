package text_to_hdfs;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

public class TextToSeqfile {	
	public static void main (String args[]){
		String infilepath = args[0];
		String outfilepath = args[1];
		
		String infilename = infilepath.substring(infilepath.lastIndexOf("/")+1);
		
		String outfilename = infilename + ".seq";
		
		System.out.println(outfilename);
		BufferedReader br = null;
		String fileContent = "";
		try {
			br = new BufferedReader(new FileReader(infilepath));
			String sCurrentLine;
			while ((sCurrentLine = br.readLine()) != null) {
				fileContent += sCurrentLine;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e1) {
			e1.printStackTrace();
		} 
		
		Path path = new Path(outfilepath + "/" + outfilename);
		Writer writer = null;
		try {
			writer = SequenceFile.createWriter(fs, conf, path, 
						new Text() .getClass(),
						new Text() .getClass() );
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Text key = new Text(infilename);
		Text value = new Text(fileContent);
		    	
		try {
			writer.append(key, value);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
