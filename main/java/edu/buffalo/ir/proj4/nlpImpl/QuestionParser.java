package edu.buffalo.ir.proj4.nlpImpl;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.buffalo.ir.proj4.nlpInterface.QuestionParserInterface;
import edu.stanford.nlp.io.IOUtils;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations.TreeAnnotation;
import edu.stanford.nlp.util.CoreMap;
import edu.stanford.nlp.util.PropertiesUtils;

public class QuestionParser  implements QuestionParserInterface{

    /**
     * @param args the command line arguments
     */
	
	 String word = "";
	 String pos = "";
	 String ne = "";
	 String query = "";
	// List<CoreMap> sentences = new ArrayList<CoreMap>();
	
	
  //  public static void main(String[] args) /*throws IOException, ClassCastException, ClassNotFoundException*/ {
        // TODO code application logic here
       public ParserOutput getQuery(String text) throws IOException, ClassCastException, ClassNotFoundException
       {
            // Create the Stanford CoreNLP pipeline
    	   Properties props = new Properties();
    		  props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
    		StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
   
    ParserOutput pOutput = new ParserOutput();
    // Annotate an example document.
   
 /*   if (args.length > 0) {
      try {
		text = IOUtils.slurpFile(args[0]);
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    } else {
      text = "Who is the president elect of United States?";
    }*/
    Annotation doc = new Annotation(text);
   
    pipeline.annotate(doc);
   // sentences.clear();
    // these are all the sentences in this document
    // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
    List<CoreMap> sentences = doc.get(SentencesAnnotation.class);   
    String answerType = null;
    for(CoreMap sentence: sentences) {
    // traversing the words in the current sentence
    // a CoreLabel is a CoreMap with additional token-specific methods
        
        for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
            // this is the text of the token
           word = token.get(TextAnnotation.class);
            // this is the POS tag of the token
           pos = token.get(PartOfSpeechAnnotation.class);
            // this is the NER label of the token
           ne = token.get(NamedEntityTagAnnotation.class);
            System.out.println(word+" "+pos+" "+ne);
            
           if(!no_impPos.contains(pos))
                query = query+" "+word;
        }
        System.out.println("--------------------------------");
        // this is the parse tree of the current sentence
       // Tree tree = sentence.get(TreeAnnotation.class);
      
    // this is the Stanford dependency graph of the current sentence
    //SemanticGraph dependencies = sentence.get(CollapsedCCProcessedDependenciesAnnotation.class);
        try {
        	answerType = answer_type_detection(sentence.toString());
		} catch (ClassCastException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // to find Named entity from sentences
    }
    
    System.out.println("Query formulated : "+query +  " "+ answerType);
    //System.exit(-1);
    pOutput.setQueryText(query);
    pOutput.setAnswerType(answerType);
    /*
    // Loop over sentences in the document
    int sentNo = 0;
    for (CoreMap sentence : doc.get(CoreAnnotations.SentencesAnnotation.class)) {
      System.out.println("Sentence #" + ++sentNo + ": " + sentence.get(CoreAnnotations.TextAnnotation.class));
      // Print SemanticGraph
      System.out.println(sentence.get(SemanticGraphCoreAnnotations.EnhancedDependenciesAnnotation.class).toString(SemanticGraph.OutputFormat.LIST));
      // Get the OpenIE triples for the sentence
      Collection<RelationTriple> triples = sentence.get(NaturalLogicAnnotations.RelationTriplesAnnotation.class);
      // Print the triples
      for (RelationTriple triple : triples) {
        System.out.println(triple.confidence + "\t" +
            triple.subjectLemmaGloss() + "\t" +
            triple.relationLemmaGloss() + "\t" +
            triple.objectLemmaGloss());
        }
      System.out.println();
       } */
    return pOutput;
    }

    public static String answer_type_detection(String sentence) throws IOException, ClassCastException, ClassNotFoundException {

    	String answer_type = "";
        if (sentence.toLowerCase().matches("(who)(.*)|(whom)(.*)|(whose)(.*)")) {
            System.out.println("Answer Type predicted : " + "PERSON");
            answer_type =  "PERSON";
        }
        
        else if(sentence.toLowerCase().matches("(where)(.*)|(whence)(.*)")){
        //if(sentence.toLowerCase().matches("^Where")){
            System.out.println("Answer Type predicted : "+"LOCATION");
            answer_type = "LOCATION";
        }
        else if(sentence.toLowerCase().matches("(when)(.*)|(which date)(.*)")){
            System.out.println("Answer Type predicted : "+"DATE");
            answer_type = "DATE";
        }
        
		else  if (sentence.toLowerCase().matches("(which)(.*)")) {
            System.out.println("Answer Type predicted : " + "ORGANIZATION");
            answer_type =  "ORGANIZATION";
        }
        


        if (sentence.toLowerCase().matches("(how few)(.*)|(how great)(.*)|(how little)(.*)|(how much)(.*)|(how many)(.*)")) {
            System.out.println("Answer Type predicted : " + "NUMBER");
            answer_type = "NUMBER";
        }
//        if(sentence.toLowerCase().matches("^[who|Whom].*")){
//            System.out.println("PERSON");
//        }
        else if (sentence.toLowerCase().matches("(what)(.*)|(why)(.*)|(describe)(.*)|(define)(.*)")) {
            System.out.println("DEFINITION");
            answer_type =  "DEFINITION";
        }
        return answer_type;
    }
}
