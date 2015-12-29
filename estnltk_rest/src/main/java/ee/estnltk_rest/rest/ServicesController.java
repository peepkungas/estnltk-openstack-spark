package ee.estnltk_rest.rest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.fs.Path;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


import ee.estnltk_rest.configurations.RemoteServerConfig;
import ee.estnltk_rest.remoteinteractions.HdfsRemoteInteraction;
import ee.estnltk_rest.utils.Operations;


@RestController
public class ServicesController {
	
	private HdfsRemoteInteraction hdfsInteraction;
	private RemoteServerConfig config;
	
	public ServicesController(){
		config=new RemoteServerConfig();
		hdfsInteraction=new HdfsRemoteInteraction();
	}	
	
	@RequestMapping(method = RequestMethod.POST, value = "/**/*")
	public ResponseEntity<String> tokenization(HttpServletRequest request) 
			throws IOException{
		String documentReference;
		String service;
		String hdfsRootDirectory;
		String fileOnHDFS;
		String submitParams;
		
		service=Operations.getService(request.getRequestURI());
		if(!Operations.providedServices.contains(service)){
			return new ResponseEntity<String>("Error: The requested service is not available", 
					HttpStatus.NOT_FOUND);
		}
			
		documentReference=Operations.getDocReference(request.getRequestURI(), service);
		hdfsRootDirectory=config.getHdfsDirectory();
		fileOnHDFS=hdfsRootDirectory+documentReference+".seq";
		
		Path hdfsPath= new Path(fileOnHDFS);
		Map<String,String> services = new HashMap<String,String>();
		services.put("-"+service, service);			
		submitParams=config.getSubmitParams();
		String taskParams="";
		String fileType="seq";
		if (fileType != "html"){
			taskParams += " -isPlaintextInput";
		}
		
		try{			
			Map<String, Path> path=hdfsInteraction.applyProcessAndGetResultLocation(hdfsPath, services, submitParams, taskParams);			
			/*Read and write the processed file and return it.*/
			return new ResponseEntity<String>(path.get("out").toString(), HttpStatus.OK);
		}catch(Exception ex){
			return new ResponseEntity<String>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	/*@RequestMapping(method = RequestMethod.POST, value = "/lemmas")
	public ResponseEntity<String> lemma(HttpServletRequest request) 
			throws IOException{
		String hdfsRootDirectory;
		String fileOnHDFS;
		String submitParams;
		
		hdfsRootDirectory=config.getHdfsDirectory();
		fileOnHDFS=hdfsRootDirectory+Operations.documentReference+".seq";
		
		Path hdfsPath= new Path(fileOnHDFS);
		Map<String,String> services = new HashMap<String,String>();
		services.put("-token", "token");			
		submitParams=config.getSubmitParams();
		String taskParams="";
		String fileType="seq";
		if (fileType != "html"){
			taskParams += " -isPlaintextInput";
		}
		
		try{			
			Map<String, Path> path=hdfsInteraction.applyProcessAndGetResultLocation(hdfsPath, services, submitParams, taskParams);			
			return new ResponseEntity<String>(path.get("out").toString(), HttpStatus.OK);
		}catch(Exception ex){
			return new ResponseEntity<String>(ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}*/
}
