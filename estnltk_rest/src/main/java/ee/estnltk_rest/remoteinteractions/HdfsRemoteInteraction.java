package ee.estnltk_rest.remoteinteractions;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.HashMap;
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
		textToSeqJarLocation=config.getTextToSeqJarLocation();
		Path inputTextLocalFSPath = new Path(localFSDest);
		Path inputSeqfileLocalFSPath = new Path(inputTextLocalFSPath+".seq");
		Path inputSeqfileHdfsPath = new Path(hdfsDest +".seq");

		System.out.println("Remote text localFS dest: " + inputTextLocalFSPath);
		System.out.println("Remote seq HDFS dest: " + inputSeqfileHdfsPath);
		
		String command = "";
		String returnCode;
		
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
		command = "hadoop dfs -mkdir -p " + inputSeqfileHdfsPath.getParent();
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
		
		command = "hadoop dfs -put " + inputSeqfileLocalFSPath + " " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);				
		
		//delete from local file system
		String seqFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath).toString();
		
		String crcFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath)
				.getParent()+"/."+inputSeqfileLocalFSPath.getName()+".crc";
		command = "rm " + seqFileRmString +" "+ crcFileRmString;
		System.out.println("command:" + command);
		returnCode = executeCommandOnRemote(command);
		System.out.println("return:" + returnCode);
				
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
	private String executeCommandOnRemote(String command) throws JSchException, IOException{		
		ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
		InputStream in = channelExec.getInputStream();

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		channelExec.setCommand(command);
		channelExec.connect();
		String result = "";
		String line;
		while ((line = reader.readLine()) != null){
			result+=line;
		}
		reader.close();
		int exitStatus = channelExec.getExitStatus();
		channelExec.disconnect();
		
		return result;
	}
	private int executeCommand(String command) 
			throws JSchException, IOException{		
		ChannelExec channelExec = (ChannelExec)session.openChannel("exec");
		InputStream in = channelExec.getInputStream();

		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		channelExec.setCommand(command);
		channelExec.connect();
		String result = "";
		String line;
		while ((line = reader.readLine()) != null){
			result+=line;
		}
		reader.close();
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
		String returnCode;
		sparkProcessPyLocation=config.getSparkProcessPyLocation();
		textToSeqJarLocation=config.getTextToSeqJarLocation();
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
	public boolean removeFile(String p) throws JSchException, IOException{
		Path inputSeqfileHdfsPath=new Path(p);
		String command="";
		int returnCode;
		
		this.initRemote();
		
		command = "hadoop fs -rm " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnCode = executeCommand(command);
		System.out.println("return:" + returnCode);
		if (returnCode > 0){
			return true;
		}
		//hdfs dfs -rmr /user/hadoop/dir
		return false;
	}
	public boolean isFileExist(String p) 
			throws JSchException, IOException{
		
		Path inputSeqfileHdfsPath=new Path(p);
		String command="";
		String returnValue;
		this.initRemote();
		
		command = "hadoop fs -ls " + inputSeqfileHdfsPath;
		System.out.println("command:" + command);
		returnValue = executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		if (returnValue.length() > 0){
			System.out.println("INFO: Seqfile already exists in HDFS, skipping to processing.");
			return true;
			//return inputSeqfileHdfsPath;
		}
		/*
		if(toCheck.equals("dir"))
			command="hdfs dfs -test -d "+path.getParent();
		else if(toCheck.equals("file"))
			command="hdfs dfs -test -e "+path.getParent();
		System.out.println(command);
		int code=this.executeCommandOnRemote(command);
		System.out.println(code);
		//session.disconnect();
		if(code<0)
			return false;*/
		return false;
	}
}

