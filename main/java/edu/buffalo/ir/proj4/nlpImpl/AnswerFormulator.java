package edu.buffalo.ir.proj4.nlpImpl;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.buffalo.ir.proj4.nlpInterface.AnswerFormulatorInterface;
import edu.buffalo.ir.proj4.nlpInterface.SolrQueryInterface;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
public class AnswerFormulator implements AnswerFormulatorInterface{

	String Label = "";
	SolrQueryInterface solrQueryInterface = new SolrQuery();
	AbstractSequenceClassifier<CoreLabel> classifier;
	static Matcher m ;
	/*public static void main(String[] args) throws IOException, ClassCastException, ClassNotFoundException {
		AnswerFormulator af  = new AnswerFormulator();
		ParserOutput pOutputObj = new ParserOutput();
		pOutputObj.setAnswerType("PERSON");
		pOutputObj.setQueryText("minister India");
		af.formulateAnswer(pOutputObj);
	}*/
	
	public AnswerFormulator() throws ClassNotFoundException, ClassCastException, IOException 
	{
		
		ClassLoader classLoader = getClass().getClassLoader();
		File file = new File(classLoader.getResource(("edu/buffalo/ir/proj4/nlpImpl/english.all.3class.distsim.crf.ser.gz")).getFile());
		System.out.println(file.getPath());
		classifier = CRFClassifier.getClassifier(file.getPath());
	}
	
	public Answer formulateAnswer(ParserOutput pOutputObj) throws IOException, ClassCastException, ClassNotFoundException 
	{
		
		
		Label = pOutputObj.getAnswerType();
        System.out.println("label =========="+Label);
        HashMap<String, Integer> answerMap = new HashMap<String, Integer>();
        
        
        String final_answer = "No definite answer found";
        String jsonString = solrQueryInterface.callURL(pOutputObj.getQueryText());
        //System.out.println("\n\njsonString: " + jsonString);
        
       // String serializedClassifier = "C:\\Users\\Raman\\Documents\\NetBeansProjects\\IRproject4\\src\\irproject4\\classifiers\\english.all.3class.distsim.crf.ser.gz";
//        AbstractSequenceClassifier<CoreLabel> classifier = CRFClassifier.getClassifier(serializedClassifier);
        
        ArrayList<String> tweet_text = new ArrayList<String>();
        ArrayList<String> final_tweet_text = new ArrayList<String>();
        // Replace this try catch block for all below subsequent examples
        try {
            //JSONObject opoutput=jsonString
            JSONArray jsonArray = new JSONArray("[" + jsonString + "]");
            //System.out.println("\n\njsonArrayelementOne: " + jsonArray.length());
            //System.out.println(jsonArray.getJSONObject(0));
            JSONObject first_elementObjects = jsonArray.getJSONObject(0);
            //System.out.println(first_elementObjects.getJSONObject("response"));
            JSONObject second_elementObjects = first_elementObjects.getJSONObject("response");
            //System.out.println(second_elementObjects.getJSONArray("docs"));
            JSONArray docs = second_elementObjects.getJSONArray("docs");

            for (int i = 0; i < docs.length(); i++) {
            
                // if both event_values and event_codes are of equal length
                JSONObject tweets = (JSONObject) docs.get(i);
               // System.out.println("tweets ==== "+ tweets);
              // System.out.println("text : "+tweets.getString("tweet_text"));
                //System.out.println(tweets.optJSONArray("tweet_text").getString(0));
                tweet_text.add(tweets.optJSONArray("tweet_text").getString(0).trim());
                //System.out.println(docs.get(i));
                //valueList.add(event_codes.getString(i));
                //displayList.add(event_values.getString(i));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }

        if ("DEFINITION".equals(Label)) {
            String split_query[] = pOutputObj.getQueryText().split("\\s+");
            String entity = split_query[0].toLowerCase();
          //  String entity = "demonetisation"; // make this dynamic
            int StrCounter = 0;
            for (String str_tweet : tweet_text) {
                //String regex = "^" + entity + "+[A-Za-z,;'\"\\s]+[.?!]$";
                //String regex = "(donald trump,)[A-Za-z,;'\"\\s]+[.?!]";
                String regex = "(" + entity + ",)[A-Za-z,;'\"\\s]+[.?!]";
                //System.out.println(regex);
                Pattern p = Pattern.compile(regex);
                Matcher m = p.matcher(str_tweet.toLowerCase());
                StrCounter++;
                if (m.find() && StrCounter==1) {
                    System.out.println(m.group());
                    final_answer = m.group();
                } 
                
            }
        } else {
            for (String str_tweet : tweet_text) {
                String categorized_text = classifier.classifyToString(str_tweet.trim(), "tabbedEntities", false);
                //System.out.println(categorized_text);
                String split_text[] = categorized_text.split("\\r\\n|\\n|\\r");

                for (String line : split_text) {
                    if (!line.isEmpty()) {
                        String[] splited_WhiteSpace = line.split("\\t");
                        String name = "";
                        String label = "";
                        name = splited_WhiteSpace[0];
                        label = splited_WhiteSpace[1];
                        if (!name.isEmpty()) {
                    if (Label.equals(label)) {
                        if (answerMap.containsKey(name.toLowerCase())) {
                            int val = answerMap.get(name.toLowerCase());
                            val++;
                            answerMap.put(name.toLowerCase(), val);
                        } else {
                            answerMap.put(name.toLowerCase(), 1);
                        }
                    }
                    }
                }
                    }
            }
            //}

            //}
        }
		Map<String, Integer> sorted_answerMap = sortByValues(answerMap);
		
            Set<Entry<String, Integer>> answerSet = sorted_answerMap.entrySet();
     
        String answer_conf = "";
        Iterator iAnswerset = answerSet.iterator();
        String answer ="";
        int topTweetCounter = 0;
        for (String str_tweet : tweet_text) {
            //System.out.println("Item is: " + str_tweet);
            //if(topTweetCounter<=5 && (!str_tweet.contains("AIB") && !str_tweet.contains("Aib")))
            	if(topTweetCounter<=5)
            {
            final_tweet_text.add(str_tweet);
            topTweetCounter++;
            }
            else if(topTweetCounter>5)
            	break;
            
        }
        if (!answerSet.isEmpty() )
        {
                int mapCounter = 0;
        while (iAnswerset.hasNext() && (mapCounter < 1)) {
            Map.Entry<String, Integer> conceptMe = (Map.Entry<String, Integer>) iAnswerset.next();
            answer = conceptMe.getKey().toString();
            answer_conf = conceptMe.getValue().toString();
            final_answer   = convert_each_letter_to_uppercase(answer);
                    System.out.println("Answer : " + final_answer + " " + " ; Confidence : " + answer_conf);
                    if (mapCounter <= 5) {
                       // System.exit(-1);
                   	mapCounter++;
                    	
                    }
                    
                }
        }
        Answer ans = new Answer();
        ans.setTweetList(final_tweet_text);
        ans.setAnswerString(final_answer);
        int conf = 0;
        if(null!= answer_conf && ""!= answer_conf)
        conf = Integer.parseInt(answer_conf);
        if( conf > 100)
        	answer_conf = "100";
        ans.setConfidence(answer_conf);
        return ans;

    }

public static String convert_each_letter_to_uppercase(String str) {

        StringBuffer stringbf = new StringBuffer();
        m = Pattern.compile("([a-z])([a-z]*)",
                Pattern.CASE_INSENSITIVE).matcher(str);
        while (m.find()) {
            m.appendReplacement(stringbf,
                    m.group(1).toUpperCase() + m.group(2).toLowerCase());
        }
        return m.appendTail(stringbf).toString();
    }
    
    private static HashMap<String, Integer> sortByValues(HashMap<String, Integer> map) {
        List list = new LinkedList(map.entrySet());
        // Defined Custom Comparator here
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Comparable) ((Map.Entry) (o2)).getValue())
                        .compareTo(((Map.Entry) (o1)).getValue());
            }
        });
        // Here I am copying the sorted list in HashMap
        // using LinkedHashMap to preserve the insertion order
        HashMap<String, Integer> sortedHashMap = new LinkedHashMap<String, Integer>();
        for (Iterator it = list.iterator(); it.hasNext();) {
            Map.Entry<String, Integer> entry = (Map.Entry) it.next();
            sortedHashMap.put(entry.getKey(), entry.getValue());
        }
        return sortedHashMap;
    }
}
