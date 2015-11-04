package languagetech.models;

public class Tokenize {
	private final String text;
	private final Object words;
	private final Object sentences;
	private final Object paragraph;
	
	public Tokenize(String text, Object words, Object sentences, Object paragraph){
		this.text=text;
		this.words=words;
		this.sentences=sentences;
		this.paragraph=paragraph;
	}
	
	public String getText(){
		return text;
	}
	public Object getWords(){
		return words;
	}
	public Object getSentences(){
		return sentences;
	}
	public Object getParagraphs(){
		return paragraph;
	}
}
