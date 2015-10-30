package languagetech.rest;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import jep.Jep;
import jep.JepException;
import languagetech.models.Request;
import languagetech.models.Tokenize;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.python.util.InteractiveInterpreter;
import org.python.util.PythonInterpreter;
import org.python.core.*;



@RestController
public class TokenizerRestController {
	
	@RequestMapping("/tokens")
	public ResponseEntity<Tokenize> doTokenize(@RequestBody Request request
			) throws InvalidFormatException, IOException, JepException{
		String text="";
		if(request!=null)
			text=request.getDocument();
		else			
		 text = "JSON ei ole okei.";
		Object word_text=new Object();
		/*try(Jep jep = new Jep(false)) {
		    jep.eval("from java.lang import System");
		    jep.eval("s = 'Hello World'");
		    jep.eval("System.out.println(s)");
		    jep.eval("print(s)");
		    word_text=jep.getValue("print(s[1:-1])");
		}*/
		try(Jep jep = new Jep(false)) {		    		    
		    jep.eval("from estnltk import Text");
		    //jep.eval("text=Text('ABC DEF')");
		    jep.eval("text=Text('"+text+"')");		    
		    word_text=jep.getValue("text.word_texts");		    
		}		
		
	 
		return new ResponseEntity<Tokenize>(new Tokenize(text,word_text), HttpStatus.OK);
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