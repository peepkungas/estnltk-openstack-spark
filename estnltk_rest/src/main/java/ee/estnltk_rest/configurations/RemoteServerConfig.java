package ee.estnltk_rest.configurations;

import java.util.Properties;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;


//@Configuration
public class RemoteServerConfig {
	Resource resource;
	Properties props;
	public RemoteServerConfig(){
		try{
			resource = new ClassPathResource("remote_server.properties");
			props = PropertiesLoaderUtils.loadProperties(resource);			
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} 
	}	
	public String getLocalDirectory(){
		return props.get("local_directory").toString();		
	}
	public String getHdfsDirectory(){
		return props.get("hdfs_directory").toString();
	}
	
	public String getHost(){
		return props.get("host").toString();
	}
	
	public int getHostProt(){
		return Integer.parseInt(props.get("host_port").toString());
	}
	
	public int getSshPort(){
		return Integer.parseInt(props.get("ssh_port").toString());
	}
	public String getUserName(){
		return props.get("user_name").toString();
	}
	
	public String getPassword(){
		return props.get("password").toString();
	}
	
	public String getTextToSeqJarLocation(){
		return props.get("textToSeqJarLocation").toString();
	}
	
	public String getSparkProcessPyLocation(){
		return props.get("sparkProcessPyLocation").toString();
	}	
	
	public String getSubmitParams(){
		return props.get("submitParams").toString();
	}
}
