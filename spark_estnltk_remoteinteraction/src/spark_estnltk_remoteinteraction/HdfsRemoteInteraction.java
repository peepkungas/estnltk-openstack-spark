package spark_estnltk_remoteinteraction;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
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



	public void init() throws IOException, URISyntaxException, JSchException{
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


	/**
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
		String returnValue;
		
		// HDFS -- check if file is in cluster hdfs
		command = "hadoop fs -ls " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		if (returnValue.length() > 0){
			System.out.println("INFO: Seqfile already exists in HDFS, skipping to processing.");
			return inputSeqfileHdfsPath;
		}
		
		// if not:
			//   SHELL(wget) -- download inputUri to cluster local FS
		command = "curl " + inputUri + " --create-dirs -o " + inputTextLocalFSPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
			// SHELL(java) -- convert input into sequencefile 
		String textToSeqArgString = inputTextLocalFSPath +" "+ inputTextLocalFSPath.getParent();
		command = "java -jar "+ textToSeqJarLocation +" "+ textToSeqArgString;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);		
		
			//   SHELL(hdfs) --  add from cluster local to cluster HDFS
		command = "hadoop fs -mkdir -p " + inputSeqfileHdfsPath.getParent();
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
		command = "hadoop fs -put " + inputSeqfileLocalFSPath + " " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
			// SHELL -- delete seqfile (and crc) from local filesystem
		String seqFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath).toString();
		
		String crcFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath)
				.getParent()+"/."+inputSeqfileLocalFSPath.getName()+".crc";
		command = "rm " + seqFileRmString +" "+ crcFileRmString;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
		//TODO: probably no need to run every time
			// SHELL -- delete empty directories
		command = "find "+ this.getLocaldir() + " -type d -empty -delete";
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
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
	
	

	
	
	/**
	 * Applies ESTNLTK Spark processing on the input file in HDFS.
	 */
	public Map<String, Path> applyProcessAndGetResultLocation(Path inputSeqfileHdfsPath, Map<String, String> estnltkOpMap, String submitParams, String taskParams) throws JSchException, IOException {
		String command = "";
		String returnValue;
		String suffix = ".result";

		Path hdfsDirectoryPath = inputSeqfileHdfsPath.getParent();
		String inputFileName = inputSeqfileHdfsPath.getName().substring(0, inputSeqfileHdfsPath.getName().length()-4);
		Path sparkProcessFinalOutHdfsPath = new Path(hdfsDirectoryPath+"/"+inputFileName+suffix);
		Map<String, Path> outputFileFinalHdfsPathMap = new HashMap<String,Path>();

		// HDFS -- check if results exist in cluster hdfs
		command = "hadoop fs -ls " + sparkProcessFinalOutHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		if (returnValue.length() > 0){
			System.out.println("INFO: Result file already exists.");
			outputFileFinalHdfsPathMap.put("out",sparkProcessFinalOutHdfsPath);
			return outputFileFinalHdfsPathMap;
		}
		// if not:
		
		
		//   SHELL?(spark) --  run spark on input, save results to hdfs
		Path sparkProcessOutHdfsPath = new Path(inputSeqfileHdfsPath + ".outdir");
		String opParamString = estnltkOpMap.keySet().toString().replace(",", " ");
		opParamString = opParamString.substring(1, opParamString.length()-1);
		command = "spark-submit " + submitParams + " " + sparkProcessPyLocation + " "+ inputSeqfileHdfsPath +" "+ sparkProcessOutHdfsPath + " " + opParamString +" "+ taskParams;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
	
		//   HDFS --  rename results to proper form
		Path sparkProcessTempOutHdfsPath = new Path(sparkProcessOutHdfsPath+"/part-00000");
		command = "hadoop fs -mv "+ sparkProcessTempOutHdfsPath +" "+ sparkProcessFinalOutHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
	
		outputFileFinalHdfsPathMap.put("out",sparkProcessFinalOutHdfsPath);
				
//		estnltkOpMap.forEach(
//			(opParam,opDir)-> 
//			outputFileFinalHdfsPathMap.put(opParam, convertAndMoveResult(opDir, sparkProcessOutHdfsPath, inputFileName))
//		);
		
		// HDFS -- verify result file existence
		command = "hadoop fs -ls " + sparkProcessFinalOutHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		if (returnValue.length() == 0){
			System.out.println("ERROR: Result file does not exist");
			return null;
		}
		
		// HDFS --  remove out directory from hdfs
		command = "hadoop fs -rm -r -f "+ sparkProcessOutHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		
		// return location(s) map of results
		return outputFileFinalHdfsPathMap;
	}

	/**
	 * Moves and renames spark output files to the correct location. 
	 */
	public Path convertAndMoveResult (String opDir, Path hdfsDirectoryPath, String inputFileName){
		String outputFileName = "part-00000";
		Path outputFileTempHdfsPath = new Path(hdfsDirectoryPath +"/"+ opDir +"/"+ outputFileName);
		String outputFinalSuffix = opDir;
		Path outputFileFinalHdfsPath = new Path(hdfsDirectoryPath +"/"+ inputFileName +"."+ outputFinalSuffix);
		try{
			String command = "hadoop fs -mv "+ outputFileTempHdfsPath +" "+ outputFileFinalHdfsPath;
			System.out.println("command:" + command);
			String returnValue = executeCommandOnRemote(command);
			System.out.println("return:" + returnValue);
			return outputFileFinalHdfsPath;
		}
		catch (Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Executes command on remote server via ssh.
	 */
	public String executeCommandOnRemote(String command) throws JSchException, IOException{
		ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
		InputStream in = channelExec.getInputStream();
		channelExec.setCommand(command);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));

		channelExec.connect();
				
		String result = "";
		String line;
		while ((line = reader.readLine()) != null){
			result+=line;
//			System.out.println("RET:"+line);
		}
		reader.close();
		int exitStatus = channelExec.getExitStatus();
		channelExec.disconnect();
		
		return result;
		
//		System.out.println("RES:"+result.length());
//		if (result.length() > 0){
//			return result;
//		}
//		else{
//			return String.valueOf(exitStatus);
//		}
	}

	/**
	 * Server local file system directory used as a buffer to download the target URI content into, then converted into Hadoop Sequencefile, then uploaded to HDFS. 
	 */
	public String getLocaldir() {
		return localdir;
	}

	/**
	 * Server local file system directory used as a buffer to download the target URI content into, then converted into Hadoop Sequencefile, then uploaded to HDFS. 
	 */
	public void setLocaldir(String localdir) {
		this.localdir = localdir;
	}

	/**
	 * IP of server.
	 */
	public String getHost() {
		return host;
	}

	/**
	 * IP of server.
	 */
	public void setHost(String host) {
		this.host = host;
	}

	/**
	 * Port of SSH connection to server.
	 */
	public int getSshport() {
		return sshport;
	}

	/**
	 * Port of SSH connection to server.
	 */
	public void setSshport(int sshport) {
		this.sshport = sshport;
	}

	/**
	 * Server user name.
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Server user name.
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * Password of server user.
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Password of server user.
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * Directory on Hadoop File System (HDFS) where sequencefiles and processing results are stored. Specified directory is used as a root directory, subdirectories are created for each different input.
	 */
	public String getHdfsdir() {
		return hdfsdir;
	}

	/**
	 * Directory on Hadoop File System (HDFS) where sequencefiles and processing results are stored. Specified directory is used as a root directory, subdirectories are created for each different input.
	 */
	public void setHdfsdir(String hdfsdir) {
		this.hdfsdir = hdfsdir;
	}

	/**
	 * Path on the server local file system to TextToSeqfile.java, used to convert textfiles into sequence files for processing.
	 */
	public String getTextToSeqJarLocation() {
		return textToSeqJarLocation;
	}

	/**
	 * Path on the server local file system to TextToSeqfile.java, used to convert textfiles into sequence files for processing.
	 */
	public void setTextToSeqJarLocation(String textToSeqJarLocation) {
		this.textToSeqJarLocation = textToSeqJarLocation;
	}

	/**
	 * Path on the server local file system to process.py, which executes ESTNLTK processes in Spark. 
	 */
	public String getSparkProcessPyLocation() {
		return sparkProcessPyLocation;
	}

	/**
	 * Path on the server local file system to process.py, which executes ESTNLTK processes in Spark. 
	 */
	public void setSparkProcessPyLocation(String sparkProcessPyLocation) {
		this.sparkProcessPyLocation = sparkProcessPyLocation;
	}

	/**
	 * JSch shell connection session to the server.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * JSch shell connection session to the server.
	 */
	public void setSession(Session session) {
		this.session = session;
	}


}

