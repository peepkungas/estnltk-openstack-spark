package languagetech.models;

public class Tokenize {
	private final String text;
	private final Object words;
	
	public Tokenize(String text, Object words){
		this.text=text;
		this.words=words;
	}
	
	public String getText(){
		return text;
	}
	public Object getObject(){
		return words;
	}
}
