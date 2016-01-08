package ee.estnltk_rest.rest;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
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
	
	@RequestMapping("/uploads")
	public ResponseEntity<String> uploadFile(@RequestBody Request request,
			HttpServletResponse response, HttpServletRequest httpRequest) 
			throws IOException, InterruptedException, JSchException{
		
		String inputFileType;
		String inputFileUri;
		String fileOnHDFS;
		String localHFDest;
		String cleanedInputUri;
				
		inputFileType=request.getMimeType();
		if(inputFileType == null || inputFileType.trim().isEmpty()){
			return new ResponseEntity<String>(new String("Message: Please specify document type in "
					+ " the request body. Check API description for document uploads."),
					HttpStatus.UNSUPPORTED_MEDIA_TYPE);
		}
		if(! Operations.acceptableTypes.contains(inputFileType)){
			return new ResponseEntity<String>(new String("Message: Specified document type is not "
					+ " supported by the service. Check API description suported types."),
					HttpStatus.UNSUPPORTED_MEDIA_TYPE);
		}
		
		inputFileUri=request.getDocumentUrl();		
		if(inputFileUri==null || inputFileUri.trim().isEmpty()){
			return new ResponseEntity<String>(new String("Message: File not Found. "
					+ "Please provide a valid address for document to upload."),
					HttpStatus.NOT_FOUND);
		}
		if(!Operations.isRemoteFileExits(inputFileUri)){
			return new ResponseEntity<String>(new String("Message: File not Found. "
					+ "Please check if file exist on the given URI."),
					HttpStatus.NOT_FOUND);
		}
		
		cleanedInputUri = inputFileUri.replace("//", "/").replace(":", "").replace("?", "_").replace("=", "_");		
		fileOnHDFS=config.getHdfsDirectory()+"/"+cleanedInputUri;
		
		/*Check if file already exist on HDFS and return 409-Conflict */
		if(hdfsInteraction.isFileExist(fileOnHDFS+".seq")){
			return new ResponseEntity<String>(new String("Message: File already exist."),
					HttpStatus.CONFLICT);
		}
		localHFDest= config.getLocalDirectory()+"/"+cleanedInputUri;
		
		hdfsInteraction.saveFromUriToRemoteHdfs(inputFileUri, localHFDest, fileOnHDFS);
		
		if(hdfsInteraction.getReturnMsg().equals("SUCCESS")){
			response.addHeader("Reference", cleanedInputUri);
        	return new ResponseEntity<String>("Message: The file is uploaded Successfully. "
        			+ "The document reference sent in header.", 
        			HttpStatus.OK);
		}
        else{
        	return new ResponseEntity<String>("Message: Failed to upload file,"
        			+ " please try again later.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
	}
}
