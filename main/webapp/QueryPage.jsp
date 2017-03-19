<%@ page contentType="text/html; charset=UTF-8"%>
<%@ taglib prefix="s" uri="/struts-tags"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Questions & Answers</title>
<link rel="stylesheet" type="text/css" href="../css/lite-blue-theme.css">
</head>
<body>


	<div>
	<div class="row">
		<div class="small-12 medium-4 columns">
			<p class="text-center">
				<i class="fa fa-2x">Answer :</i>
			</p>
			
			<div class="text-center fa fa-lg" >
			<s:property value="finalAnswer" ></s:property>
			</div>
			
			
		</div>
		<div class="small-12 medium-4 columns">
			
			<p class="text-center">
				<i class="fa fa-2x">Confidence % :</i>
			</p>
			<div class="text-center fa fa-lg">
			<s:property value="confidence" ></s:property>
			</div>
			
		</div>
		<div class="small-12 medium-4 columns">
			<p class="text-center">
				<i class="fa fa-2x">Top Tweets :</i>
			</p>
			<s:iterator var="i" step="1" value="tweetList">
				<tr>
					<td><ul><li>
					<s:property></s:property></li></ul></td>
					<td></td>
					<td></td>
				</tr>
			</s:iterator>
			<!-- <div class="small-12 medium-4 columns">
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
		</div> -->
		</div>
		


	</div>








</body>
</html>