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
	public void saveFromUriToRemoteHdfs(String inputUri, String localFSDest, String hdfsDest) 
			 {
		try{
			
			this.initRemote();
			textToSeqJarLocation=config.getTextToSeqJarLocation();
			//Path inputTextDownloadingPath=new Path(localFSDest+".part");
			Path inputTextLocalFSPath = new Path(localFSDest);
			Path inputSeqfileLocalFSPath = new Path(inputTextLocalFSPath+".seq");
			Path inputSeqfileHdfsPath = new Path(hdfsDest);	
			
			String command = "";
			String sshValue;
			
			/*create corresponding directories on local file system and download remote file to local*/
			command = "curl " + inputUri + " --create-dirs -o " + inputTextLocalFSPath;
			sshValue = executeCommandOnRemote(command);
						
			/*Convert to the input file into sequence file*/
			String textToSeqArgString = inputTextLocalFSPath +" "+ inputTextLocalFSPath.getParent();
			command = "java -jar "+ textToSeqJarLocation +" "+ textToSeqArgString;
			sshValue = executeCommandOnRemote(command);
				
			/*Add from local file system to HDFS*/			
			command = "hadoop dfs -mkdir -p " + inputSeqfileHdfsPath.getParent();
			sshValue = executeCommandOnRemote(command);
			command = "hadoop dfs -put " + inputSeqfileLocalFSPath + " " + inputSeqfileHdfsPath;
			sshValue = executeCommandOnRemote(command);
			
			/*delete from local file system*/
			String seqFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath).toString();
			String crcFileRmString = Path.getPathWithoutSchemeAndAuthority(inputSeqfileLocalFSPath)
					.getParent()+"/."+inputSeqfileLocalFSPath.getName()+".crc";
			command = "rm " + seqFileRmString +" "+ crcFileRmString;
			sshValue = executeCommandOnRemote(command);
			
			/*delete empty directories*/			
			command = "find "+ config.getLocalDirectory() + " -type d -empty -delete";
			sshValue = executeCommandOnRemote(command);
			
			returnMsg="Success";
		}
		catch(IOException e){
			returnMsg="IOException";
		}
		catch(JSchException e){
			returnMsg="JSchException";
		}
		catch (Exception e){
			returnMsg="Exception";
		}	
		finally{
			session.disconnect();
		}
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
		//int exitStatus = channelExec.getExitStatus();
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
		/*String result = "";
		String line;
		while ((line = reader.readLine()) != null){
			result+=line;
		}*/
		reader.close();
		int exitStatus = channelExec.getExitStatus();
		channelExec.disconnect();	
		return exitStatus;
	}
	
	/*
	 * Applies ESTNLTK Spark processing on the input file in HDFS.
	 */
	public void applyProcessAndGetResultLocation(
			Path inputSeqfileHdfsPath, Map<String, String> estnltkOpMap, 
			String submitParams, String fileType) {
		String command = "";
		String taskParams="";
		
		if (fileType != "html"){
			taskParams += " -isPlaintextInput";
		}
		try{
			this.initRemote();
			String returnCode;
			sparkProcessPyLocation=config.getSparkProcessPyLocation();
			textToSeqJarLocation=config.getTextToSeqJarLocation();
			Path hdfsDirectoryPath = inputSeqfileHdfsPath.getParent();
			String inputFileName = inputSeqfileHdfsPath.getName().substring(0, inputSeqfileHdfsPath.getName().length()-4);
			String suffix = ".result";
			Path sparkProcessFinalOutHdfsPath = new Path(hdfsDirectoryPath+"/"+inputFileName+suffix);
			
			/* HDFS -- check if results exist in cluster hdfs*/
			command = "hadoop fs -ls " + sparkProcessFinalOutHdfsPath;
			System.out.println("command:" + command);
			returnCode = executeCommandOnRemote(command);
			System.out.println("return:" + returnCode);
			if (returnCode.length() > 0){
				command = "hadoop fs -rm " + sparkProcessFinalOutHdfsPath;
				System.out.println("command:" + command);
				returnCode = executeCommandOnRemote(command);
			}
			
			Path sparkProcessOutHdfsPath = new Path(inputSeqfileHdfsPath + ".outdir");
			String opParamString = estnltkOpMap.keySet().toString().replace(",", " ");
			opParamString = opParamString.substring(1, opParamString.length()-1);
			command = "spark-submit " + submitParams + " " + sparkProcessPyLocation + " "+ inputSeqfileHdfsPath +" "+ sparkProcessOutHdfsPath + " " + opParamString +" "+ taskParams;
			returnCode = executeCommandOnRemote(command);
			
			//   HDFS --  rename results to proper form
			Map<String, Path> outputFileFinalHdfsPathMap = new HashMap<String,Path>();
			
			
			Path sparkProcessTempOutHdfsPath = new Path(sparkProcessOutHdfsPath+"/part-00000");
			command = "hadoop fs -mv "+ sparkProcessTempOutHdfsPath +" "+ sparkProcessFinalOutHdfsPath;
			System.out.println("command:" + command);
			returnCode = executeCommandOnRemote(command);
			System.out.println("return:" + returnCode);
		
			outputFileFinalHdfsPathMap.put("out",sparkProcessFinalOutHdfsPath);
			
			// HDFS --  remove out directory from hdfs
			command = "hadoop fs -rm -r -f "+ sparkProcessOutHdfsPath;
			returnCode = executeCommandOnRemote(command);
		}
		catch(JSchException e){}
		catch(IOException e){}
		catch(Exception e){}
		finally{
			session.disconnect();
		}
	}
	public String readResult(String hdfsDest) 
			throws IOException, JSchException{
		String command;
		this.initRemote();
		command = "hadoop fs -text "+hdfsDest;
		return executeCommandOnRemote(command);		
	}

	public boolean isFileExist(String p, String fileSystem) 
			throws JSchException, IOException{
		
		Path filePath=new Path(p);
		String command="";
		String returnValue;
		this.initRemote();
		if(fileSystem.trim().equals("local"))
			command="ls "+filePath;
		else if(fileSystem.trim().equals("remote"))
			command = "hadoop fs -ls " + filePath;
		
		returnValue = executeCommandOnRemote(command);
		if (returnValue.length() > 0){
			return true;
		}		
		return false;
	}
}

