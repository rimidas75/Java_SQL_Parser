package edu.buffalo.ir.proj4;

import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import edu.buffalo.ir.proj4.nlpImpl.Answer;
import edu.buffalo.ir.proj4.nlpImpl.AnswerFormulator;
import edu.buffalo.ir.proj4.nlpImpl.ParserOutput;
import edu.buffalo.ir.proj4.nlpImpl.QuestionParser;
import edu.buffalo.ir.proj4.nlpInterface.AnswerFormulatorInterface;
import edu.buffalo.ir.proj4.nlpInterface.QuestionParserInterface;

public class WelcomeAction {

	private String question;
	public List<String> tweetList; 
	private String finalAnswer;
	private String confidence;
	

	public String execute() throws Exception {
		   tweetList = new ArrayList<String>();
		   Answer ansObj = new Answer();
		   //assuming question gives the keyword to be searched for
		   //HttpSolrServer solr = new HttpSolrServer("http://localhost:8983/solr/test_core_5");
		  QuestionParserInterface qIntf = new QuestionParser();
		   ParserOutput pOut =   qIntf.getQuery(question);
		   // SolrQuery query = new SolrQuery();
		   AnswerFormulatorInterface ansFormulated = new AnswerFormulator();
		    System.out.println("here" +pOut.getAnswerType());
		   // String queryString = "text_en:"+pOut.getQueryText();
		    ansObj =  ansFormulated.formulateAnswer(pOut);
		    tweetList = ansObj.getTweetList();
		    finalAnswer = ansObj.getAnswerString();
		    confidence = ansObj.getConfidence();
		    
		  //  query.setQuery(queryString);
		   // //query.addFilterQuery("cat:electronics","store:amazon.com");
		   // query.setFields("id","price","merchant","cat","store");
		    //query.set("q", "tweet_hashtags:Syria");
		    //query.setStart(0);    
		    //query.set("defType", "edismax");
/*
		    QueryResponse response = solr.query(query);
		    SolrDocumentList results = response.getResults();
		    for (int i = 0; i < results.size(); ++i) {
		    	SolrDocument dd = results.get(i);
		    	tweetList.add((dd.getFieldValue("text_en")).toString());
		    }
		    */
		   
		   
	      return "success";
	   }


	public String getFinalAnswer() {
		return finalAnswer;
	}


	public void setFinalAnswer(String finalAnswer) {
		this.finalAnswer = finalAnswer;
	}


	public String getConfidence() {
		return confidence;
	}


	public void setConfidence(String confidence) {
		this.confidence = confidence;
	}


	public String getQuestion() {
		return question;
	}

	public void setQuestion(String question) {
		this.question = question;
	}

	public List<String> getTweetList() {
		return tweetList;
	}

	public void setTweetList(List<String> tweetList) {
		this.tweetList = tweetList;
	}
	   
}
