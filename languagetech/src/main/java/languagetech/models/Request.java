package languagetech.models;

public class Request {
	String document;
	String docUrl;
	String mimeType;
	String[] annotations;
	
	public Request(){
		document="";
		docUrl="";
		mimeType="";
		annotations=new String[10];
	}
	public Request(String d, String du, String mt, String[] a){
		this.document=d;
		this.docUrl=du;
		this.mimeType=mt;
		this.annotations=a;
	}
	
	public String getDocument(){
		return document;
	}
	public String getDocUrl() {
		return docUrl;
	}
	public String getMimeType() {
		return mimeType;
	}
	public String[] getAnnotations() {
		return annotations;
	}
	
	
}