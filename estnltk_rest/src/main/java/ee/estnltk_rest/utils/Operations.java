package ee.estnltk_rest.utils;

import java.util.Arrays;
import java.util.List;


public class Operations {
	
	public static final List<String> acceptableTypes= 
			Arrays.asList(new String[]{"text/plain", "text/html", "text/sequence"});
	
	
	public static String getDocReference(String requestURI, String service) {	
		StringBuilder reference= new StringBuilder(requestURI.replace(service, ""));
		reference.deleteCharAt(reference.length()-1);
		return reference.toString();
	}
	public static String getService(String requestURI) {	
		String[] requestStrs=requestURI.split("/");
		return requestStrs[requestStrs.length-1];		
	}

}