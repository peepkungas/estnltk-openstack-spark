package spark_estnltk_remoteinteraction;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class HdfsRemoteInteractionExample {
	//XXX: TESTING ONLY
	public void runFullProcess() throws JSchException, IOException, InterruptedException{
		// TODO: get from config
		HdfsRemoteInteraction hdfsInteractor = new HdfsRemoteInteraction();
		hdfsInteractor.setLocaldir("");
		hdfsInteractor.setHdfsdir("");
		hdfsInteractor.setHost("");
		hdfsInteractor.setSshport(22);
		hdfsInteractor.setUsername("");
		hdfsInteractor.setPassword("");
		hdfsInteractor.setTextToSeqJarLocation("");
		hdfsInteractor.setSparkProcessPyLocation("");

			//watch out for script injection (purify URI beforehand)
			//remove trailing "/"-symbol
//		String inputUri = "http://textfiles.com/games/abbrev.txt";
		String inputUri = "http://www.cs.ut.ee/et/teadus/uurimisruhmad";
		String cleanedInputUri = inputUri.replace("//", "/").replace(":", "_").replace("?", "_").replace("=", "_");
		String localFSDest = "file://" + hdfsInteractor.getLocaldir() + "/" + cleanedInputUri;
		String hdfsDest = "hdfs://"+ hdfsInteractor.getHdfsdir() + "/" + cleanedInputUri;
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
			hdfsInteractor.init();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// Save from input URI to HDFS, get HDFS location
		Path hdfsPath = null;		
		Session session = hdfsInteractor.getSession();
		session.connect();
		if(session == null || !session.isConnected()){
			System.out.println("Unable to establish SSH connection, closing.");
			return;
		}
		hdfsPath = hdfsInteractor.saveFromUriToRemoteHdfs(inputUri, localFSDest, hdfsDest);

		System.out.println("SEQFILE LOC: " + hdfsPath);

		// Apply ESTNLTK processes to seqfile, get result location
		Map<String, Path> processResultLocationsMap = hdfsInteractor.applyProcessAndGetResultLocation(hdfsPath, estnltkOpMap, submitParams, taskParams);
		System.out.println("OUTMAP: "+ processResultLocationsMap);
		
		// DEBUG: delete result parent directory with results in it
		System.out.println("DELETING RESULTS!");
		String command = "hadoop fs -rm -r -f "+ processResultLocationsMap.get("out").getParent();
		System.out.println("command:" + command);
		String returnValue = hdfsInteractor.executeCommandOnRemote(command);
		System.out.println("return:" + returnValue);
		// END DEBUG
		
		session.disconnect(); //closes session, needs reinitializing before reopen?
		
	}
	

	public static void main(String[] args) throws JSchException, IOException, InterruptedException{
		HdfsRemoteInteractionExample interactionExample = new HdfsRemoteInteractionExample();
		interactionExample.runFullProcess();
	}
}
