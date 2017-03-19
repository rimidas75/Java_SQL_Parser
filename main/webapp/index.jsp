<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
   pageEncoding="ISO-8859-1"%>
<%@ taglib prefix="s" uri="/struts-tags"%>
<!DOCTYPE html>

<html class="no-js" lang="en" data-useragent="Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>IR Question & Answer</title>
<meta name="description" content="SailBoat, FREE Foundation 5 website template.">
<meta name="keywords" content="foundation 5 template, sailboat template, free template">
<link rel="stylesheet" href="css/foundation.css">
<link rel="stylesheet" href="css/app.css">
<link rel="stylesheet" href="css/font-awesome.min.css">
<script src="js/modernizr.js"></script>
<meta class="foundation-data-attribute-namespace">
<meta class="foundation-mq-xxlarge">
<meta class="foundation-mq-xlarge">
<meta class="foundation-mq-large">
<meta class="foundation-mq-medium">
<meta class="foundation-mq-small">
<meta class="foundation-mq-topbar">
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
    start_img.src = "http://findicons.com/files/icons/1580/devine_icons_part_2/128/mic_w.png";
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
	 /*  $('#loading-image').bind('ajaxStart', function(){
		    $(this).show();
		}).bind('ajaxStop', function(){
		    $(this).hide();
		}); */
       function search() {
          var title = $("#question").val();
          if (title != "") {
              $("#result").html("Loading");
              $('#loading-image').show();
              $.ajax({
                  type: "post",
                  url: "hello",
                  data: "question=" + title,
                  success: function (data) {
                      $("#result").html(data);
                     // $("#question").val("");
                      $('#loading-image').hide();
                      $('#Welcome').hide();
                      
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
<nav class="top-bar foundation-bar" data-topbar>
	<ul class="title-area">
		<li class="name">
			<span data-tooltip class="has-tip" title="Free Foundation 5 website template"><h1><a href="#">Q & A</a></h1></span>
		</li>
		<li class="toggle-topbar menu-icon"><a href="#"><span>menu</span></a></li>
	</ul>
	<section class="top-bar-section">
	<!-- Right Nav Section -->
	<ul class="right">
		<li class="active"><a href="index.jsp"><i class="fa fa-home fa-lg"></i></a></li>
		<li><a href="about-template.html">IR Project 4</a></li>
		<li class="has-dropdown">
			<a href="#">Team : Total Recall</a>
			<ul class="dropdown">
				<li><label>Team Members</label></li>
				<li><a href="#">Kishlay Jha</a></li>
				<!-- <li class="divider"></li> -->
				<li><a href="#">Gourab Mitra</a></li>
				<li><a href="#">Rimi Das</a></li>
				<li><a href="#">Navneet Nandi</a></li>
			</ul>
		</li>
		<li><a href="http://www.blacksailor.com/en/contact/">Contact Us</a></li>
	</ul>
	</section>
</nav>
<div class="row">
	<div class="small-12 columns home-image">
		<div class="row">
			<div class="small-12 medium-6 columns medium-offset-3">
				<div class="blue_mark">
					<h3>Have Questions?</h3>
					<p>Enter your question below and get the answer</p>
					<s:form>
						<input type="text" name="question" id="question" placeholder="Example: Whose signature is there on the new notes ?" value ="">
						 <img alt="Start" id="start_img" name="start_img" src="http://findicons.com/files/icons/1580/devine_icons_part_2/128/mic_w.png" onclick="toggleStartStop()">
						<a class="button" id="button" href="#">Submit <i class="fa fa-lg"></i></a>
					</s:form>
				</div>
			</div>
		</div>
	</div>
</div>
<div class="row">
	<div class="small-12 columns">
		
		<div id ="loading-image" class ="loading-image"></div>
		<ul></ul>
		<ul></ul>
		<ul id="result"></ul>
		
	</div>
</div>
<!-- Featured Section -->
<div class="lighten">
	<div class="row">
	<h3 class="text-center" id="Welcome">Template Questions</h3>
		<div class="small-12 medium-4 columns">
			<p class="text-center"></p>
			<p>Who is the Prime Minister of India? </p>
			<p>Whose signature is there on the new Indian Notes? <!-- <a href="#">Lighten area anchor</a> --> </p>
		</div>
		<div class="small-12 medium-4 columns">
			<p class="text-center"></p>
			<p>What is Demonetisation? <!-- <a href="#">Lighten area anchor</a> --> </p>
			<p>Define Demonetization? <!-- <a href="#">Lighten area anchor</a> --> </p>
			<p>Describe Demonetization? <!-- <a href="#">Lighten area anchor</a> --> </p>
		</div>
		<div class="small-12 medium-4 columns">
			<p class="text-center"></i></p>
			<p>Which government introduced demonetization?</p>
			<p>When did PM Modi introduce demonetization?</p>
			<p>How many people died during demonetization?</p>
		</div>
	</div>
</div>
<!-- <div class="row">
	<div class="small-12 columns">
		<h3>Lorem ipsum dolor sit amet</h3>
	</div>
	<div class="small-12 medium-6 columns">
		<p><img src="images/lake.jpg" /></p>
		<p>Fusce pulvinar lacus lacus. Pellentesque a malesuada leo, a laoreet nulla. Mauris ac turpis euismod nunc aliquam blandit. Morbi tempor vulputate mi, ac malesuada enim commodo sit amet. </p>
		<p class="text-right"><a href="#"><i class="fa fa-angle-double-right"></i> Read more</a></p>
	</div>
	<div class="small-12 medium-6 columns">
		<p><img src="images/boat.jpg" /></p>
		<p>Fusce pulvinar lacus lacus. Pellentesque a malesuada leo, a laoreet nulla. Mauris ac turpis euismod nunc aliquam blandit. Morbi tempor vulputate mi, ac malesuada enim commodo sit amet. </p>
		<p class="text-right"><a href="#"><i class="fa fa-angle-double-right"></i> Read more</a></p>
	</div>
</div> -->
<!-- Blue Section -->
<!-- <div class="blue">
	<div class="row">
	<p class="text-center"><i class="fa fa-umbrella fa-4x">TOP TWEETS</i></p>
		<div class="small-12 medium-4 columns">
			<p class="text-center"><i class="fa fa-umbrella fa-4x"></i></p>
			<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum non dolor ultricies, porttitor justo non.</p>
		</div>
		<div class="small-12 medium-4 columns">
			<p class="text-center"><i class="fa fa-magic fa-4x"></i></p>
			<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. <a href="#">Blue area anchor</a> Vestibulum non dolor, porttitor justo non.</p>
		</div>
		<div class="small-12 medium-4 columns">
			<p class="text-center"><i class="fa fa-line-chart fa-4x"></i></p>
			<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Vestibulum non dolor ultricies, porttitor justo non.</p>
		</div>
	</div>
</div> -->
<div class="prefooter">
	<div class="row">
		<div class="medium-3 columns">
			<h6>Latest news</h6>
			<ul class="no-bullet">
				<li><a href="#">Demonetization in India</a></li>
				<li><a href="#">Currency Ban in India</a></li>
				<li><a href="#">President Elect of US: Donald Trump</a></li>
				
			</ul>
		</div>
		<div class="medium-3 columns">
			<!-- <h6></h6>
			<p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Cras finibus sapien velit, vitae placerat sem malesuada at.</p>
			<p>Mauris vel sapien finibus, maximus elit vel, blandit purus. Vestibulum scelerisque dui nec ligula.</p> -->
		</div>
		
		<div class="medium-4 columns">
			<h6>Stay in touch</h6>
			<p>Enter your email address to receive monthly updates and highlights. Read Last <a href="#">Month's Edition</a></p>
			<div class="row">
				<div class="large-12 columns">
				  <div class="row collapse">
					<div class="small-9 columns">
					  <input type="text" placeholder="Your email address">
					</div>
					<div class="small-3 columns">
					  <a href="#" class="button postfix">Submit</a>
					</div>
				  </div>
				</div>
			  </div>
		</div>
	</div>
</div>
<div class="footer">
	<div class="row">
		<div class="large-12 columns">
			<div class="row">
				<div class="medium-6 columns">
					<p class="pad1">&copy; 2016 UB CSE IR. All rights reserved.</p>
				</div>
				<div class="small-12 medium-6 columns">
					<ul class="inline-list right pad1">
						<li><a href="#"><i class="fa fa-rss fa-2x"></i></a></li>
						<li><a href="#"><i class="fa fa-facebook fa-2x"></i></a></li>
						<li><a href="#"><i class="fa fa-twitter fa-2x"></i></a></li>
						<li><a href="#"><i class="fa fa-google-plus fa-2x"></i></a></li>
						<li><a href="#"><i class="fa fa-instagram fa-2x"></i></a></li>
						<li><a href="#"><i class="fa fa-pinterest fa-2x"></i></a></li>
					</ul>
				</div>
				<!-- <div class="small-12 columns">
					<p class="text-right">Tempate created by <a href="http://blacksailor.com"></a></p>
				</div> -->
			</div>
		</div>
	</div>
</div>
<script src="js/jquery.js"></script>
<script src="js/foundation.min.js"></script>
<script>
	$(document).foundation();
</script>
</body>
</html>
