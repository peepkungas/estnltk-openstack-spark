package ee.estnltk_rest.utils;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.List;


public class Operations {
	
	public static final List<String> acceptableTypes= 
			Arrays.asList(new String[]{"text/plain", "text/html", "text/sequence"});
	public static final List<String> providedServices= 
			Arrays.asList(new String[]{"tokens", "lemmas", "pos-tags", "morph-analysis", "named-entities",
					"text-contents"});
	
	public static String getDocReference(String requestURI, String service) {	
		StringBuilder reference= new StringBuilder(requestURI.replace(service, ""));
		reference.deleteCharAt(reference.length()-1);
		return reference.toString();
	}
	public static String getService(String requestURI) {	
		String[] requestStrs=requestURI.split("/");
		return requestStrs[requestStrs.length-1];		
	}
	public static boolean isRemoteFileExits(String fileURI){
		try {
		      HttpURLConnection.setFollowRedirects(false);
		      // note : you may also need
		      //        HttpURLConnection.setInstanceFollowRedirects(false)
		      HttpURLConnection con =
		         (HttpURLConnection) new URL(fileURI).openConnection();
		      con.setRequestMethod("HEAD");
		      return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
		    }
		    catch (Exception e) {
		       e.printStackTrace();
		       return false;
		    }
	}

}