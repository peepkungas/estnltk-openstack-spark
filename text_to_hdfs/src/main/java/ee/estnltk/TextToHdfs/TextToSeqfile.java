package ee.estnltk.TextToHdfs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextToSeqfile {

    private static final Logger LOG = LoggerFactory.getLogger(TextToSeqfile.class);

    public static void main(String args[]) {

        if (args.length < 1) {
            System.out.println("Usage: text_to_hdfs <inputfilepath> <outputfilepath>");
            System.exit(0);
        }

        String infilepath = args[0];
        String outfilepath = args[1];
        String infilename = infilepath.substring(infilepath.lastIndexOf("/") + 1);
        String outfilename = infilename + ".seq";
        BufferedReader br = null;
        String fileContent = "";

        try {
            br = new BufferedReader(new FileReader(infilepath));
            String sCurrentLine;
            while ((sCurrentLine = br.readLine()) != null) {
                fileContent += sCurrentLine;
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }

        Configuration conf = getConfiguration();
        FileSystem fs = getFileSystem(outfilepath, conf);
        Path path = new Path(outfilepath + "/" + outfilename);
        Writer writer = null;

        try {
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            writer = SequenceFile.createWriter(fs, conf, path, new Text().getClass(), new Text().getClass());
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }

        Text key = new Text(infilename);
        Text value = new Text(fileContent);

        try {
            writer.append(key, value);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }

        try {
            writer.close();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    private static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        return conf;
    }

    private static FileSystem getFileSystem(String outfilepath, Configuration conf) {
        try {
            if (outfilepath.startsWith("hdfs://")) {
                return FileSystem.get(new URI(outfilepath), conf);
            } else {
                return FileSystem.get(conf);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (URISyntaxException e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
