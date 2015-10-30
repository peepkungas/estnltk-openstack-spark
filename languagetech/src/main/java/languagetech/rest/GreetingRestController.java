package languagetech.rest;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicLong;

import languagetech.models.Greeting;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingRestController {
	
	private static final String template="Salam, %s!";
	private final AtomicLong counter=new AtomicLong();
	
	/*@RequestMapping("/tokenizer")
	public String[] tokenizing(@RequestParam(value="text", defaultValue="No. Senctence.") String text) throws InvalidFormatException, IOException{
		
		InputStream is = new FileInputStream("src/main/java/languagetech/en-sent.bin");
		SentenceModel model = new SentenceModel(is);
		SentenceDetectorME sdetector = new SentenceDetectorME(model);
	 
		String sentences[] = sdetector.sentDetect("This is a complete Sentence. I think it is now well defined");
	 
		return sentences;
	}*/
	
	@RequestMapping("/greeting")
	public Greeting greeting(@RequestParam(value="name", defaultValue="World") String name){
		return new Greeting(counter.incrementAndGet(),
					String.format(template, name));
		
	}
	
	
	
}
