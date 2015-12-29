package ee.estnltk_rest.models;

public class UploadResponse {
	private String message;
	private String documentReferece;	
	
	public void setMessage(String message){
		this.message=message;
	}
	public void setDocumentReference(String documentRefernce){
		this.documentReferece=documentRefernce;
	}
	
	public String getMessage(){
		return message;
	}
	public String getDocumentReference(){
		return documentReferece;
	}

}
