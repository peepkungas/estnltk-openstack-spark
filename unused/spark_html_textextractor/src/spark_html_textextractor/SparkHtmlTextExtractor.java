package spark_html_textextractor;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.kohlschutter.boilerpipe.BoilerpipeProcessingException;
import com.kohlschutter.boilerpipe.extractors.ArticleExtractor;


public final class SparkHtmlTextExtractor {
	/* input  : Sequencefile or folder of Sequencefiles containing key-value pairs, where each value is unparsed html
	 * output : RDD containing key-value pairs. Key is unmodified key from Sequencefile, value is parsed html.

	/* Process:
	 * 	get key-value pairs from inputfile
	 * 		for each key-value pair, create Spark task:
	 * 			parse html
	 * 			save key-value pair with parsed html
	 * 	combine results
	 * 	return key-value pairs
	 */

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: SparkHtmlTextExtractor <inputpath>");
			System.exit(1);
		} 

		SparkConf sparkConf = new SparkConf().setAppName("HtmlTextExtractor");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		String path = args[0];

		// get key-value pairs from Sequencefile
		JavaPairRDD<Text,Text> keyvals = ctx.sequenceFile(path, Text.class, Text.class);
		
//		keyvals.saveAsTextFile("unparsedoutput");
		
		// parse each value
		@SuppressWarnings("serial")
		JavaPairRDD<Text, Text> parsedkeyvals = keyvals.mapValues(
				new Function<Text,Text>() {
					public Text call(Text val){
						//Parse html (val) into plaintext
						Text parsedText = null;
						try {
							parsedText = new Text(ArticleExtractor.INSTANCE.getText(val.toString()));
						} catch (BoilerpipeProcessingException e) {
							e.printStackTrace();
						}
						return parsedText;
					}
				});
		
		//parsedkeyvals.persist(null);
		parsedkeyvals.saveAsTextFile("testoutput");
		
		ctx.close();
	}
}



// ALTERNATIVE // TESTING

// extract key-value pairs from each file
//JavaPairRDD<Text,Text> keyvals = filenames.flatMapToPair(path -> ctx.sequenceFile(path, Text.class, Text.class));
	
//JavaRDD<JavaPairRDD<Text, Text>> keyvals = filenames.map(
//		new Function<String,JavaPairRDD<Text,Text> >(){
//			public JavaPairRDD<Text, Text> call(String path){
//				JavaPairRDD<Text, Text> keyvals = ctx.sequenceFile(path, Text.class, Text.class);
//				return keyvals;
//			}
//		});
