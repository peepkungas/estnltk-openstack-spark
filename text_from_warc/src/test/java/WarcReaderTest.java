import warcreader.WarcReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

/**
 * Created by Madis-Karli Koppel on 2/11/2017.
 */
public class WarcReaderTest {

    private String inputPath = "C:\\Users\\Joonas Papoonas\\Documents\\IR\\testdata";

    private String outputPath = "C:\\Users\\Joonas Papoonas\\Documents\\IR\\testdata\\testout";

    @Before
    public void setup() {
        System.setProperty("spark.master", "local[2]");
    }

    @After
    public void deleteOutput() throws IOException {
        FileUtils.deleteDirectory(new File(outputPath));
    }

    // Test if the program shows message about parameters
    @Test(expected = IOException.class)
    public void noArgs() throws IOException {
        String[] args = new String[0];
        WarcReader.main(args);
    }

//    //  Program should fail with null args
//    @Test(expected = NullPointerException.class)
//    public void noPaths() throws IOException {
//        WarcReader.main(new String[2]);
//    }

    // TODO Tests that actually read files
    // Test PDF
    // Test txt
    // Test docx
    // Test Excel

//    // Test dns
//    @Test
//    public void readDns() throws Exception{
//        String dir = "onlydns";
//        String[] params = new String[2];
//        params[0] = inputPath + "\\" + dir;
//        params[1] = outputPath + "\\" + dir;
//
//        WarcReader.main(params);
//
//        //Check if the file is empty
//        String contents = fileContents(outputPath, dir);
//
//        if(!contents.equals("")){
//            throw new Exception("Output file not empty!");
//        }
//    }

//    // TODO update if more info
//    // Test html
//    @Test
//    public void readHtml() throws Exception{
//        String dir = "onlyhtml";
//
//        String[] params = new String[2];
//        params[0] = inputPath + "\\" + dir;
//        params[1] = outputPath + "\\" + dir;
//
//        WarcReader.main(params);
//
//        //Check if the file is empty
//        String contents = fileContents(outputPath, dir);
//
//        if(!contents.equals("")){
//            throw new Exception("Output file not empty! Contains: " + contents);
//        }
//    }
//
    private String fileContents(String outputpath, String dirWildcard) throws Exception{
        // Find the output directory
        File dir = new File(outputpath);
        FileFilter fileFilter = new WildcardFileFilter(dirWildcard + "*");
        File[] dirs  = dir.listFiles(fileFilter);

        if(dirs.length != 1){
            throw new Exception("Too many output directories, expected one. Did you run the test and did not delete the file? Dir: " + outputpath);
        }

        File directory = dirs[0];
        fileFilter = new WildcardFileFilter("part*");
        File[] outFiles = directory.listFiles(fileFilter);

        if(outFiles.length != 1){
            throw new Exception("Too many output files, expected one. Did you run the test and did not delete the file? Dir: " + outputpath + "\\" + directory.toString());
        }

        File aFile = outFiles[0];

        BufferedReader br = new BufferedReader(new FileReader(aFile));
        String contents;
        try {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();

            while (line != null) {
                sb.append(line);
                sb.append(System.lineSeparator());
                line = br.readLine();
            }
            contents = sb.toString();
        } finally {
            br.close();
        }

        return contents;
    }
}
