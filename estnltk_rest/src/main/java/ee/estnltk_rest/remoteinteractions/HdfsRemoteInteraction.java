package ee.estnltk_rest.remoteinteractions;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import ee.estnltk_rest.configurations.RemoteServerConfig;

public class HdfsRemoteInteraction {
	private Session session;
	private String textToSeqJarLocation;
	private String sparkProcessPyLocation;
	
	private String returnMsg;
	private RemoteServerConfig config;
	
	public HdfsRemoteInteraction(){
		config=new RemoteServerConfig();
	}



	private void init() throws IOException, URISyntaxException, JSchException{

		JSch jsch = new JSch();
		Session session = jsch.getSession(config.getUserName(), config.getHost(), config.getSshPort());
		session.setConfig("StrictHostKeyChecking", "no");
		session.setPassword(config.getPassword());
		this.session = session;
	}
	private void initRemote() throws JSchException{
		// init class fields
		try {
			init();
			this.session.connect();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		
	}

	/*
	 * Downloads the inputUri to remote local file system, converts it to Hadoop sequencefile, then copies it from local to HDFS.
	 * returns: Path to sequencefile in HDFS if successful 
	 */
	public Path saveFromUriToRemoteHdfs(String inputUri, String localFSDest, String hdfsDest) 
			throws IOException, JSchException{
		
		this.initRemote();
		returnMsg = "NO_MSG";
		
		Path inputTextLocalFSPath = new Path(localFSDest);
		Path inputSeqfileLocalFSPath = new Path(inputTextLocalFSPath+".seq");
		Path inputSeqfileHdfsPath = new Path(hdfsDest +".seq");

		System.out.println("Remote text localFS dest: " + inputTextLocalFSPath);
		System.out.println("Remote seq HDFS dest: " + inputSeqfileHdfsPath);
		
		String command = "";
		int returnCode;
		
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
		session.disconnect();
		return inputSeqfileHdfsPath;
	}
	
	public String getReturnMsg(){
		return returnMsg;
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
	public Map<String, Path> applyProcessAndGetResultLocation(Path inputSeqfileHdfsPath, Map<String, String> estnltkOpMap, String submitParams, String taskParams) throws JSchException, IOException {
		String command = "";
		this.initRemote();
		int returnCode;
		sparkProcessPyLocation=config.getSparkProcessPyLocation();
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
		
		// HDFS --  remove out directory from hdfs
		command = "hadoop fs -rm -r -f "+ sparkProcessOutHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
		session.disconnect();
		
		// return location(s) map of results
		return outputFileFinalHdfsPathMap;
	}
	
	/*public void runFullProcess(String fileuri, String fileType, String[] services) 
			throws JSchException, IOException, InterruptedException{		
		
			//watch out for script injection (purify URI beforehand)
			//remove trailing "/"-symbol
//		String inputUri = "http://textfiles.com/games/abbrev.txt";
		String inputUri = fileuri;//"http://www.cs.ut.ee/et/teadus/uurimisruhmad";
		String localFSDest = "file://" + localdir + "/" + cleanedInputUri; //into the server local file system
		String hdfsDest = "hdfs://"+ hdfsdir + "/" + cleanedInputUri;		//into the hdfs
		String type = fileType;//"html";
		String submitParams = "--master yarn-cluster --num-executors 4";
		String taskParams = "";
		Map<String,String> estnltkOpMap = new HashMap<String,String>();
		for(String service: services)
			estnltkOpMap.put("-".concat(service), service);//("-lemma","lemma"); 
		//estnltkOpMap.put("-ner","ner");
		
		// construct Spark process params
		if (type != "html"){
			taskParams += " -isPlaintextInput";
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
		
	}*/
}

