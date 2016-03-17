package ee.estnltk.SequencefilesToHdfs;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoveSequencefilesToHdfs {

    private static final Logger LOG = LoggerFactory.getLogger(MoveSequencefilesToHdfs.class);

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: moveSequenceToHdfs <inputpath> <outputpath>");
            System.exit(0);
        }

        String inputpath = args[0];
        String outputpath = args[1];
        Configuration conf = getConfiguration();
        FileSystem fs = getFileSystem(outputpath, conf);

        List<String> listFiles = new ArrayList<String>();
        File path = new File(inputpath);

        for (File file : path.listFiles()) {
            listFiles.add(file.getAbsolutePath());
        }

        for (String file : listFiles) {
            if (file.endsWith(".seq")) {
                String filename = file.substring(file.lastIndexOf("/") + 1);
                String outputFile = outputpath + "/" + filename;
                System.out.println("Moving file: " + file + " to: " + outputFile);
                try {
                    fs.moveFromLocalFile(new Path(file), new Path(outputFile));
                } catch (IllegalArgumentException | IOException e) {
                    LOG.error(e.getMessage());
                    try {
                        movefailedFile(file, inputpath);
                    } catch (IOException ex) {
                        LOG.error(ex.getMessage());
                        throw new RuntimeException("File: '" + file + "' could not be moved.\n" + e.getMessage());
                    }
                }
            }
        }
    }

    private static void movefailedFile(String file, String inputpath) throws IOException {
        String filename = file.substring(file.lastIndexOf("/") + 1);
        String meta = "." + filename + ".crc";
        String metafile = file.replace(filename, meta);
        File failed = new File(inputpath + "/.failed/" + filename);
        File failedMeta = new File(inputpath + "/.failed/" + meta);

        FileUtils.moveFile(new File(file), failed);
        if (Files.exists(Paths.get(metafile))) {
            FileUtils.moveFile(new File(metafile), failedMeta);
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