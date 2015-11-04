package languagetech.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import jep.Jep;
import jep.JepException;
import languagetech.models.Request;
import languagetech.models.Tokenize;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.docStructureDocBuilder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TokenizerRestController {
	boolean doInit = true;
	Jep jep;
	
	
	@RequestMapping("/tokens")
	public ResponseEntity<Object> doTokenize(@RequestBody Request request
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
		Object sentences=null;
		Object paragraph=null;
		try{
				jep = new Jep(true);
				jep.eval("import platform");
				jep.eval("platform.machine()");
				jep.eval("from estnltk import Text");
			    jep.eval("text=Text('"+text+"')");	
			    List<String> annotations=Arrays.asList(request.getAnnotationSelectors());
			    if(annotations.size()>0){			    	
			    	if(annotations.contains("words"))
			    		word_text=jep.getValue("text.word_texts");
			    	if(annotations.contains("sentences"))
			    		sentences=jep.getValue("text.sentence_texts");
			    	if(annotations.contains("paragraphs"))
			    		paragraph=jep.getValue("text.paragraph_texts");
			    }
		}catch(Exception ex){
			return fireError(ex);
		}
		
		return new ResponseEntity<Object>(new Tokenize(text,word_text, sentences, paragraph), HttpStatus.OK);
//		return new ResponseEntity<Object>(new Tokenize("Text ABC","Text"), HttpStatus.OK);
	}

	private ResponseEntity<Object> fireError(Exception ex) {
		String error=ex.toString();
		return new ResponseEntity<Object>(error, HttpStatus.BAD_REQUEST);	
		
	}
}


/***********--OLD--**********/
//return word_text;
/*PythonInterpreter interpreter=new PythonInterpreter();
System.out.println("Salam! This is Python Java hybride Jython!!");
interpreter.exec("print('This is python from java')");
interpreter.exec("import sys");
interpreter.exec("print sys");*/
			
/*InputStream is = new FileInputStream("files/en-sent.bin");
SentenceModel model = new SentenceModel(is);
SentenceDetectorME sdetector = new SentenceDetectorME(model);
		
String sentences[] = sdetector.sentDetect(text);*/