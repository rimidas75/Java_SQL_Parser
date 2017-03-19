package edu.buffalo.ir.proj4.nlpImpl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;

import edu.buffalo.ir.proj4.nlpInterface.SolrQueryInterface;

public class SolrQuery implements SolrQueryInterface	{
	
	
	public  String callURL(String myURL) {
		myURL = myURL.trim();
		myURL = myURL.replaceAll(" ","%20");
		myURL = "http://54.208.57.80:8984/solr/IRP4/select?indent=on&q="+myURL+"&rows=10000&wt=json";
        System.out.println("Requested URL:" + myURL);
        StringBuilder sb = new StringBuilder();
        URLConnection urlConn = null;
        InputStreamReader in = null;
        try {
            URL url = new URL(myURL);
            urlConn = url.openConnection();
            if (urlConn != null) {
                urlConn.setReadTimeout(60 * 1000);
            }
            if (urlConn != null && urlConn.getInputStream() != null) {
                in = new InputStreamReader(urlConn.getInputStream(),
                        Charset.defaultCharset());
                BufferedReader bufferedReader = new BufferedReader(in);
                if (bufferedReader != null) {
                    int cp;
                    while ((cp = bufferedReader.read()) != -1) {
                        sb.append((char) cp);
                    }
                    bufferedReader.close();
                }
            }
            in.close();
        } catch (Exception e) {
            throw new RuntimeException("Exception while calling URL:" + myURL, e);
        }

        return sb.toString();
    }
	
	
	

}
