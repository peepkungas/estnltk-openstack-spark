package ee.estnltk_rest.rest;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import ee.estnltk_rest.utils.Operations;

@RestController
public class DocUploadController {
	
	private RemoteServerConfig config;
	private HdfsRemoteInteraction hdfsInteraction;
	
	public DocUploadController(){
		config=new RemoteServerConfig();
		hdfsInteraction=new HdfsRemoteInteraction();
	}
	
	@RequestMapping(method = RequestMethod.POST, value ="/documents")
	public ResponseEntity<String> uploadFile(@RequestBody Request request,
			HttpServletResponse response, HttpServletRequest httpRequest) 
			throws InterruptedException, JSchException, IOException{
		
		String inputFileType;
		String inputFileUri;
		String fileOnHDFS;
		String localHFDest;
		String cleanedInputUri;
				
		inputFileType=request.getMimeType();
		if(inputFileType == null || inputFileType.trim().isEmpty()){
			return new ResponseEntity<String>(new String("Error: Please specify document type in "
					+ " the request body. Check API description for document uploads."),
					HttpStatus.BAD_REQUEST);
		}
		if(! Operations.acceptableTypes.contains(inputFileType)){
			return new ResponseEntity<String>(new String("Error: Specified document type is not "
					+ " supported by the service. Check API description for supported types."),
					HttpStatus.UNSUPPORTED_MEDIA_TYPE);
		}
		
		inputFileUri=request.getDocumentUrl();		
		if(inputFileUri==null || inputFileUri.trim().isEmpty()){
			return new ResponseEntity<String>(new String("Error: File URI not provided. "
					+ "Please provide a valid address for document to upload."),
					HttpStatus.BAD_REQUEST);
		}
		if(!Operations.isRemoteFileExits(inputFileUri)){
			return new ResponseEntity<String>(new String("Error: File not found. "
					+ "Please check if file exist on the given URI."),
					HttpStatus.NOT_FOUND);
		}
		
		cleanedInputUri = inputFileUri.replace("//", "/").replace(":", "").replace("?", "_").replace("=", "_");		
		fileOnHDFS=config.getHdfsDirectory()+"/"+cleanedInputUri+".seq";
		
		/*Check if file already exist on HDFS and return 409-Conflict */
		if(hdfsInteraction.isFileExist(fileOnHDFS, "remote")){
			return new ResponseEntity<String>(new String("Error: File already exist."),
					HttpStatus.CONFLICT);
		}
		localHFDest= config.getLocalDirectory()+"/"+cleanedInputUri;
		
		Runnable myrunnable = new Runnable() {
		    public void run() {
		    	hdfsInteraction.saveFromUriToRemoteHdfs(inputFileUri, localHFDest, fileOnHDFS);
		    }
		};
		new Thread(myrunnable).start();
				
		response.addHeader("Reference", cleanedInputUri);
        return new ResponseEntity<String>("Message: File is uploading. Use document reference for further "
        		+ "communication.", 
        			HttpStatus.OK);		
	}
	
	@RequestMapping(method = RequestMethod.GET, value ="/**/status")
	public ResponseEntity<String> getFileUploadStatus(HttpServletResponse response, 
			HttpServletRequest request) 
			throws IOException, InterruptedException, JSchException{
		
		HttpStatus httpStatus;
		String fileStatus="";
		String documentReference;
		String fileOnHDFS;
		String fileOnLocal;
		
		documentReference=Operations.getDocReference(request.getRequestURI(), "status");
		if(documentReference==null || documentReference.trim().isEmpty()){			
			return new ResponseEntity<String>(new String("Error: Document reference not specified."),
					HttpStatus.BAD_REQUEST);
		}
		
		fileOnHDFS=config.getHdfsDirectory()+documentReference;		
		fileOnLocal=config.getLocalDirectory()+documentReference;
		if(hdfsInteraction.isFileExist(fileOnHDFS+".result", "remote")){
			fileStatus="Status: Document is processed.";
			httpStatus=HttpStatus.OK;
		}
		else if(hdfsInteraction.isFileExist(fileOnHDFS+".seq", "remote")){
			fileStatus="Status: Document is ready for process.";
			httpStatus=HttpStatus.OK;
		}
		else if(hdfsInteraction.isFileExist(fileOnLocal, "local")){
			fileStatus="Status: Document is currently uploading to our system.";
			httpStatus=HttpStatus.OK;
		}
		else{
			fileStatus="Error: Document not found.";
			httpStatus=HttpStatus.NOT_FOUND;
		}
		return new ResponseEntity<String>(fileStatus, httpStatus);
	}
}
