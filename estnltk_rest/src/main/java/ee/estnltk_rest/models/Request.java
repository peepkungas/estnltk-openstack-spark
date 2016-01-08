package ee.estnltk_rest.models;

public class Request {
	String documentUrl;
	String mimeType;
	
	public Request(){
		documentUrl="";
		mimeType="";
		
	}
	public Request(String d, String du, String mt){
		this.documentUrl=du;
		this.mimeType=mt;
	}
	
	public String getDocumentUrl() {
		return documentUrl;
	}
	public String getMimeType() {
		return mimeType;
	}	
}