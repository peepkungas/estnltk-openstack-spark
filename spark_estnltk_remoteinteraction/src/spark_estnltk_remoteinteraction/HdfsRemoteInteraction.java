package spark_estnltk_remoteinteraction;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.security.UserGroupInformation;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class HdfsRemoteInteraction {
	private Configuration conf = null;
	private DFSClient client = null;
	private FileSystem fileSystem = null;
	private String localdir;
	private String host;
	private int hdfsport;
	private int sshport;
	private String username;
	private String password;
	private Session session;
	private String hdfsdir;
	private String textToSeqJarLocation;
	private String sparkProcessPyLocation;



	private void init() throws IOException, URISyntaxException, JSchException{
//		this.conf = new Configuration();
//		String serverUriString = "hdfs://"+host+":"+hdfsport;
//		this.conf.set("fs.defaultFS", serverUriString);
//		this.client = new DFSClient(new URI(serverUriString), conf);
//		this.fileSystem = FileSystem.get(conf);
		
		JSch jsch = new JSch();
		Session session = jsch.getSession(username, host, sshport);
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(password);
		this.session = session;
	}


	/*
	 * Downloads the inputUri to remote local file system, converts it to Hadoop sequencefile, then copies it from local to HDFS.
	 * returns: Path to sequencefile in HDFS if successful 
	 */
	public Path saveFromUriToRemoteHdfs(String inputUri, String localFSDest, String hdfsDest) 
			throws IOException, JSchException{
		String returnMsg = "NO_MSG";
		
		Path inputTextLocalFSPath = new Path(localFSDest);
		Path inputSeqfileLocalFSPath = new Path(inputTextLocalFSPath+".seq");
		Path inputSeqfileHdfsPath = new Path(hdfsDest +".seq");

		System.out.println("Remote text localFS dest: " + inputTextLocalFSPath);
		System.out.println("Remote seq HDFS dest: " + inputSeqfileHdfsPath);
		
		String command = "";
		int returnCode;
		
		// HDFS -- check if file is in cluster hdfs
		/*		if ( fileSystem.exists(dstPath)){
			return "FILE_EXISTS";
		}*/
		// if not:
			//   SHELL(wget) -- download inputUri to cluster local FS
		command = "curl " + inputUri + " --create-dirs -o " + inputTextLocalFSPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
			// SHELL(java) -- convert input into sequencefile 
		//TODO: convert to seqfile command
		String textToSeqArgString = inputTextLocalFSPath +" "+ inputTextLocalFSPath.getParent();
		command = "java -jar "+ textToSeqJarLocation +" "+ textToSeqArgString;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);		
			//   SHELL(hdfs) --  add from cluster local to cluster HDFS
		command = "hadoop fs -mkdir -p " + inputSeqfileHdfsPath.getParent();
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
		command = "hadoop fs -put " + inputSeqfileLocalFSPath + " " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
		
		//TODO: delete from local filesystem
		
		returnMsg = "SUCCESS";
		/*
		Path localPath = new Path(host+":"+(sshport)+"//"+localLoc);
		fileSystem.copyFromLocalFile(localPath, localFSPath);
		if ( fileSystem.exists(localFSPath)){
			return "FILE_CREATED";
		}
		else {
			return "COULD_NOT_CREATE";	
		}*/
		return inputSeqfileHdfsPath;
	}
	
	
	/*
	 * Executes command on remote server via ssh.
	 */
	private int executeCommandOnRemote(String command) throws JSchException, IOException{

		
		ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
		InputStream in = channelExec.getInputStream();
		channelExec.setCommand(command);
		channelExec.connect();
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		List<String> result = new ArrayList<String>();
		String line;
		while ((line = reader.readLine()) != null){
			result.add(line);
			System.out.println(line);
		}
		int exitStatus = channelExec.getExitStatus();
		channelExec.disconnect();

		return exitStatus;
	}

	
	
	/*
	 * Applies ESTNLTK Spark processing on the input file in HDFS.
	 */
	private Map<String, Path> applyProcessAndGetResultLocation(Path inputSeqfileHdfsPath, Map<String, String> estnltkOpMap, String submitParams, String taskParams) throws JSchException, IOException {
		String command = "";
		int returnCode;
		Path hdfsDirectoryPath = inputSeqfileHdfsPath.getParent();
		String inputFileName = inputSeqfileHdfsPath.getName().substring(0, inputSeqfileHdfsPath.getName().length()-4);
		
		// HDFS -- check if lemmas are in cluster hdfs
		// if not:
		//   SHELL?(spark) --  run spark on input, save results to hdfs
		Path sparkProcessOutHdfsPath = new Path(inputSeqfileHdfsPath + ".outdir");
		String opParamString = estnltkOpMap.keySet().toString().replace(",", " ");
		opParamString = opParamString.substring(1, opParamString.length()-1);
		command = "spark-submit " + submitParams + " " + sparkProcessPyLocation + " "+ inputSeqfileHdfsPath +" "+ sparkProcessOutHdfsPath + " " + opParamString +" "+ taskParams;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
	
		//   HDFS --  rename results to proper form
		Map<String, Path> outputFileFinalHdfsPathMap = new HashMap<String,Path>();
		String suffix = ".result";
		Path sparkProcessTempOutHdfsPath = new Path(sparkProcessOutHdfsPath+"/part-00000");
		Path sparkProcessFinalOutHdfsPath = new Path(hdfsDirectoryPath+"/"+inputFileName+suffix);
		command = "hadoop fs -mv "+ sparkProcessTempOutHdfsPath +" "+ sparkProcessFinalOutHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
	
		outputFileFinalHdfsPathMap.put("out",sparkProcessFinalOutHdfsPath);
				
//		estnltkOpMap.forEach(
//			(opParam,opDir)-> 
//			outputFileFinalHdfsPathMap.put(opParam, convertAndMoveResult(opDir, sparkProcessOutHdfsPath, inputFileName))
//		);
		
		// HDFS --  remove out directory from hdfs
		command = "hadoop fs -rm -r -f "+ sparkProcessOutHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
		// return location(s) map of results
		return outputFileFinalHdfsPathMap;
	}

	/*
	 * Moves and renames spark output files to the correct location. 
	 */
	private Path convertAndMoveResult (String opDir, Path hdfsDirectoryPath, String inputFileName){
		String outputFileName = "part-00000";
		Path outputFileTempHdfsPath = new Path(hdfsDirectoryPath +"/"+ opDir +"/"+ outputFileName);
		String outputFinalSuffix = opDir;
		Path outputFileFinalHdfsPath = new Path(hdfsDirectoryPath +"/"+ inputFileName +"."+ outputFinalSuffix);
		try{
			String command = "hadoop fs -mv "+ outputFileTempHdfsPath +" "+ outputFileFinalHdfsPath;
			System.out.println("command:" + command);
			int returnCode = executeCommandOnRemote(command);
			System.out.println("return:" + returnCode);
			return outputFileFinalHdfsPath;
		}
		catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	//XXX: TESTING ONLY
	public void runFullProcess() throws JSchException, IOException, InterruptedException{
		// TODO: get from config
		this.localdir = "";
		this.hdfsdir = "";
		this.host = "";
		this.hdfsport = 8020;
		this.sshport = 22;
		this.username = "";
		this.password = "";
		this.textToSeqJarLocation = "";
		this.sparkProcessPyLocation = "";
		
			//watch out for script injection (purify URI beforehand)
			//remove trailing "/"-symbol
//		String inputUri = "http://textfiles.com/games/abbrev.txt";
		String inputUri = "http://www.cs.ut.ee/et/teadus/uurimisruhmad";
		String cleanedInputUri = inputUri.replace("//", "/").replace(":", "_").replace("?", "_").replace("=", "_");
		String localFSDest = "file://" + localdir + "/" + cleanedInputUri;
		String hdfsDest = "hdfs://"+ hdfsdir + "/" + cleanedInputUri;
		String type = "html";
		String submitParams = "--master yarn-cluster --num-executors 4";
		String taskParams = "";
		Map<String,String> estnltkOpMap = new HashMap<String,String>();
		estnltkOpMap.put("-lemma","lemma");
		estnltkOpMap.put("-ner","ner");
		
		// construct Spark process params
		if (type != "html"){
			taskParams += " -isPlaintextInput";
		}
		
		// init class fields
		try {
			init();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// Save from input URI to HDFS, get HDFS location
		Path hdfsPath = null;		
		session.connect();

		hdfsPath = saveFromUriToRemoteHdfs(inputUri, localFSDest, hdfsDest);

		System.out.println("SEQFILE LOC: " + hdfsPath);

		// Apply ESTNLTK processes to seqfile, get result location
		Map<String, Path> processResultLocationsMap = applyProcessAndGetResultLocation(hdfsPath, estnltkOpMap, submitParams, taskParams);
		System.out.println("OUTMAP: "+ processResultLocationsMap);
		session.disconnect(); //closes session, needs reinitializing before reopen?
		
	}



	public String getLocaldir() {
		return localdir;
	}


	public void setLocaldir(String localdir) {
		this.localdir = localdir;
	}


	public String getHost() {
		return host;
	}


	public void setHost(String host) {
		this.host = host;
	}


	public int getSshport() {
		return sshport;
	}


	public void setSshport(int sshport) {
		this.sshport = sshport;
	}


	public String getUsername() {
		return username;
	}


	public void setUsername(String username) {
		this.username = username;
	}


	public String getPassword() {
		return password;
	}


	public void setPassword(String password) {
		this.password = password;
	}


	public String getHdfsdir() {
		return hdfsdir;
	}


	public void setHdfsdir(String hdfsdir) {
		this.hdfsdir = hdfsdir;
	}


	public String getTextToSeqJarLocation() {
		return textToSeqJarLocation;
	}


	public void setTextToSeqJarLocation(String textToSeqJarLocation) {
		this.textToSeqJarLocation = textToSeqJarLocation;
	}


	public String getSparkProcessPyLocation() {
		return sparkProcessPyLocation;
	}


	public void setSparkProcessPyLocation(String sparkProcessPyLocation) {
		this.sparkProcessPyLocation = sparkProcessPyLocation;
	}


	public static void main(String[] args) throws JSchException, IOException, InterruptedException{
		HdfsRemoteInteraction interactor = new HdfsRemoteInteraction();
		interactor.runFullProcess();
	}

}

