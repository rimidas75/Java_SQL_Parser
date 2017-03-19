package edu.buffalo.ir.proj4.nlpInterface;

import java.io.File;
import java.io.IOException;
import java.util.ResourceBundle;

import edu.buffalo.ir.proj4.nlpImpl.Answer;
import edu.buffalo.ir.proj4.nlpImpl.ParserOutput;

public interface AnswerFormulatorInterface {
	/*ResourceBundle mybundle = ResourceBundle.getBundle("english.all.3class.distsim.crf.ser.gz");
	
	//File absolute = new File("english.all.3class.distsim.crf.ser.gz");
	static String serializedClassifier = mybundle.getBaseBundleName();
	*/
//	static String serializedClassifier = "./english.all.3class.distsim.crf.ser.gz";
	public Answer formulateAnswer(ParserOutput pOutputObj) throws IOException, ClassCastException, ClassNotFoundException ;

}
