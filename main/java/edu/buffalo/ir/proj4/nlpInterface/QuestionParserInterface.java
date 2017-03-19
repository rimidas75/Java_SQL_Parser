package edu.buffalo.ir.proj4.nlpInterface;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import edu.buffalo.ir.proj4.nlpImpl.ParserOutput;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.PropertiesUtils;

public interface QuestionParserInterface {
/*	File absolute = new File("english.all.3class.distsim.crf.ser.gz");
	static String serializedClassifier = absolute.getPath();*/
/*	ResourceBundle mybundle = ResourceBundle.getBundle("english.all.3class.distsim.crf.ser.gz");
	
	//File absolute = new File("english.all.3class.distsim.crf.ser.gz");
	static String serializedClassifier = mybundle.getBaseBundleName();*/
//	static String serializedClassifier = "./english.all.3class.distsim.crf.ser.gz";;
	
	public static final String[] SET_VALUES = new String[] { "VBD", "IN", "WP", "DT", "PRP", "PRP$", "VBZ", ".", "WRB",
			"WDT", "WP$", "RB", "JJ" };
	public static final Set<String> no_impPos = new HashSet<String>(Arrays.asList(SET_VALUES));

	/*public static final Properties props = PropertiesUtils.asProperties(
	            "annotators", "tokenize,ssplit,pos,lemma,depparse,natlog,openie"
	    );*/
	
	
	public ParserOutput getQuery(String text) throws IOException, ClassCastException, ClassNotFoundException;

	
}
