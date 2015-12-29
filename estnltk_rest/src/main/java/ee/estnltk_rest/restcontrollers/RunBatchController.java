package ee.estnltk_rest.restcontrollers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.fs.Path;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.jcraft.jsch.JSchException;

import ee.estnltk_rest.configurations.RemoteServerConfig;
import ee.estnltk_rest.models.Request;
import ee.estnltk_rest.remoteinteractions.HdfsRemoteInteraction;


@RestController
public class RunBatchController {
	
	private HdfsRemoteInteraction hdfsInteraction;
	RemoteServerConfig config;
	
	public RunBatchController(){
		config=new RemoteServerConfig();
		hdfsInteraction=new HdfsRemoteInteraction();
	}
	
	@RequestMapping("/uploads")
	public ResponseEntity<Object> uploadFile(@RequestBody Request request) 
			throws IOException, InterruptedException, JSchException{
		
		String inputFileUri=request.getDocUrl();		
		String cleanedInputUri = inputFileUri.replace("//", "/").replace(":", "").replace("?", "_").replace("=", "_");
		String localHFDest= config.getLocalDirectory()+"/"+cleanedInputUri;
		String hdfsDirectory=config.getHdfsDirectory()+"/"+cleanedInputUri;
		
		hdfsInteraction.saveFromUriToRemoteHdfs(inputFileUri, localHFDest, hdfsDirectory);
		
		if(hdfsInteraction.getReturnMsg().equals("SUCCESS"))		
        	return new ResponseEntity<Object>(cleanedInputUri, HttpStatus.OK);
        else
        	return new ResponseEntity<Object>("FAILURE", HttpStatus.INTERNAL_SERVER_ERROR);	
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/**/tokens")
	public ResponseEntity<String> tokenization(HttpServletRequest request) throws IOException{
		try{			
			String docReference=getDocReference(request.getRequestURI(), "tokens");	
			
			String hdfsRootDirectory=config.getHdfsDirectory();
			
			Path hdfsPath= new Path(hdfsRootDirectory+docReference+".seq");
			Map<String,String> services = new HashMap<String,String>();
			services.put("-token", "token");
			String submitParams=config.getSubmitParams();
			String taskParams="";
			String fileType="seq";
			if (fileType != "html"){
				taskParams += " -isPlaintextInput";
			}
			Map<String, Path> path=hdfsInteraction.applyProcessAndGetResultLocation(hdfsPath, services, submitParams, taskParams);			
			return new ResponseEntity<String>(path.get("out").toString(), HttpStatus.OK);
		}catch(Exception ex){
			return new ResponseEntity<String>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@RequestMapping(method = RequestMethod.POST, value = "/**/lemmas")
	public ResponseEntity<String> lemma(HttpServletRequest request) throws IOException{
		try{			
			String docReference=getDocReference(request.getRequestURI(), "lemmas");	
			
			String hdfsRootDirectory=config.getHdfsDirectory();
			
			Path hdfsPath= new Path(hdfsRootDirectory+docReference+".seq");
			Map<String,String> services = new HashMap<String,String>();
			services.put("-lemma", "lemma");
			String submitParams=config.getSubmitParams();
			String taskParams="";
			String fileType="seq";
			if (fileType != "html"){
				taskParams += " -isPlaintextInput";
			}
			Map<String, Path> path=hdfsInteraction.applyProcessAndGetResultLocation(hdfsPath, services, submitParams, taskParams);			
			return new ResponseEntity<String>(path.get("out").toString(), HttpStatus.OK);
		}catch(Exception ex){
			return new ResponseEntity<String>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	private String getDocReference(String requestURI, String service) {		
		StringBuilder str= new StringBuilder(requestURI.replace(service, ""));
		str.deleteCharAt(str.length()-1);
		return str.toString();
	}
	
}
