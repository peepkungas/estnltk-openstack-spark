package ee.estnltk_rest.models;

public class Request {
	String documentUrl;
	String mimeType;
	String[] annotationSelectors;
	
	public Request(){
		documentUrl="";
		mimeType="";
		annotationSelectors=new String[10];
	}
	public Request(String d, String du, String mt, String[] a){
		this.documentUrl=du;
		this.mimeType=mt;
		this.annotationSelectors=a;
	}
	
	public String getDocumentUrl() {
		return documentUrl;
	}
	public String getMimeType() {
		return mimeType;
	}
	public String[] getAnnotationSelectors() {
		return annotationSelectors;
	}
	
	
}