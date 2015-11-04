package languagetech.rest;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import jep.Jep;
import jep.JepException;
import languagetech.models.Request;
import languagetech.models.Tokenize;
import opennlp.tools.util.InvalidFormatException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MorphAnalysisController {
	boolean doInit = true;
	Jep jep;
	
	
	@RequestMapping("/morph-analysis")
	public ResponseEntity<Object> doMorphAnalysis(@RequestBody Request request
			) throws InvalidFormatException, IOException, JepException{
		String text="";
		if(request!=null){
			if(!request.getDocument().trim().equals(""))
				text=request.getDocument();
			else if(!request.getDocUrl().trim().equals("")){
				URL url = new URL(request.getDocUrl());
				Scanner s = new Scanner(url.openStream());
				while(s.hasNextLine()){
					text+=s.nextLine();
				}				
			}
		}
		else			
		 text = "JSON ei ole okei.";
		
		Object word_text=null;
		Object sentence=null;
		Object paragraph=null;
		try{
				jep = new Jep(true);
				jep.eval("import platform");
				jep.eval("platform.machine()");
				jep.eval("from estnltk import Text");
			    jep.eval("text=Text('"+text+"')");				    
			    jep.eval("a=text.tag_analysis()");
	    		word_text=jep.getValue("a");
			    
//			    List<String> annotations=Arrays.asList(request.getAnnotationSelectors());
//			    if(annotations.size()>0){			    	
//			    	if(annotations.contains("token")){			    		
//			    	}			    		
//			    	if(annotations.contains("sentence")){			    		
//			    	}
//			    }
//			    else
//			    	word_text=jep.getValue("text.get.word_texts.postags.postag_descriptions.as_dict");			    
			    
		}catch(Exception ex){
			return fireError(ex);
		}
		
		return new ResponseEntity<Object>(new Tokenize(text,word_text, sentence, paragraph), HttpStatus.OK);
//		return new ResponseEntity<Object>(new Tokenize("Text ABC","Text"), HttpStatus.OK);
	}


	private ResponseEntity<Object> fireError(Exception ex) {
		String error=ex.toString();
		return new ResponseEntity<Object>(error, HttpStatus.BAD_REQUEST);	
		
	}
}
