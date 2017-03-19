package edu.buffalo.ir.proj4.nlpImpl;

import java.util.List;

public class Answer {
	
	List<String> tweetList;
	String answerString;
	String confidence;
	
	public String getConfidence() {
		return confidence;
	}
	public void setConfidence(String confidence) {
		this.confidence = confidence;
	}
	public List<String> getTweetList() {
		return tweetList;
	}
	public void setTweetList(List<String> tweetList) {
		this.tweetList = tweetList;
	}
	public String getAnswerString() {
		return answerString;
	}
	public void setAnswerString(String answerString) {
		this.answerString = answerString;
	}
	
	

}
