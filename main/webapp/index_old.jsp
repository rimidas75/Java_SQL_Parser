<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
   pageEncoding="ISO-8859-1"%>
<%@ taglib prefix="s" uri="/struts-tags"%>
   <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" 
"http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<title>IR Q  & A</title>
<link rel="stylesheet" type="text/css" href="../css/lite-blue-theme.css">
<style>
            
</style>
<script src="//ajax.googleapis.com/ajax/libs/jquery/1.9.1/jquery.min.js" type="text/javascript"></script>
<script type="text/javascript">
if (!('webkitSpeechRecognition' in window)) {

} else { 
    var recognition = new webkitSpeechRecognition();
    recognition.continuous = true;  
    recognition.interimResults = true;  
    recognition.lang = "en-US"; 
    recognition.maxAlternatives = 1;
  var  recognizing ;
}
recognition.onstart = function() {
	//alert("start");
	recognizing =  true;
   };

recognition.onend = function() {
	//alert("end");
   
};

recognition.onresult = function(event) { 
	
	   /*  if (typeof(event.results) === 'undefined') { 
	        recognition.stop();
	        return;
	    } */

	    for (var i = event.resultIndex; i < event.results.length; ++i) {      
	       if (event.results[i].isFinal) { 
	            question.value =  event.results[i][0].transcript;   
	        } else {   
	        	question.value = question.value + event.results[i][0].transcript; 
	         }  
	    } 
	}; 


function startButton(event) {
    recognition.start();
    start_img.src = "https://speechlogger.appspot.com/images/micslash2.png";
}
function reset() {
    recognizing = false;
    start_img.innerHTML = "Click to Speak";
  }

  function toggleStartStop() {
    if (recognizing) {
      recognition.stop();
      reset();
    } else {
      recognition.start();
      recognizing = true;
      start_img.innerHTML = "Click to Stop";
      question.value = "";
      
    }
  }
  jQuery(document).ready(function ($) {
       function search() {
          var title = $("#question").val();
          if (title != "") {
              $("#result").html("Loading");
              $.ajax({
                  type: "post",
                  url: "hello",
                  data: "question=" + title,
                  success: function (data) {
                      $("#result").html(data);
                      $("#question").val("");
                  }
              });
          }
      } 
      
      /* function search() {
    	  $('#queryForm').submit(function() {
    	    $.post('<s:url action="hello" />', {
    	    	question : $('#question').val(),
    	    }, function(data) {
    	    	$("#result").html(data);
                $("#question").val("");
    	    });
    	  });
    	} */
      
      

      $("#button").click(function () {
          search();
      });

      $('#question').keyup(function (e) {
          if (e.keyCode == 13) {
              search();
          }
      });
  });
</script>
</head>
<body>
   
   <s:form  id = "queryForm" name = "queryForm" action="hello">
   <div id="container">
      <label for="name">Please enter your question</label><br/>
      <input type="text" name="question" id="question" value="" style="width: 500px;" placeholder="Example: Whose signature is there on the new notes ?"/>
      <input type="button" id="button" value="Search" />
      <div onclick="toggleStartStop()"><img alt="Start" id="start_img" name="start_img" src="https://speechlogger.appspot.com/images/micoff2.png"></div>
      <ul id="result"></ul>
        </div>
            
   </s:form>
</body>
</html>