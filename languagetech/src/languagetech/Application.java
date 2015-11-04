package languagetech;

import jep.Jep;
import jep.JepException;

public class Application {
	
	public static void main(String[] args) throws JepException{
		try(Jep jep=new Jep(false)){
			jep.eval("import platform");
			jep.eval("platform.machine()");
			jep.eval("from estnltk import Text");
		    jep.eval("text = Text('Hello World')");
		    jep.eval("text.word_texts");
		}
	}

}
