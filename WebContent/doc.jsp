
<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.0 Transitional//EN">
<HTML>
	<HEAD>
		<TITLE>Helios Reporting Framework - API Documentation</TITLE>
		<META http-equiv=Content-Type content="text/html; charset=iso-8859-1">
		<LINK href="styles/iv/index.css" type=text/css rel=stylesheet>
		<link rel="shortcut icon" href="/Framework/images/favicon.ico?v=<%= (1+Math.random())*1000%>" type="image/x-icon">
		<link rel="icon" href="/Framework/images/favicon.ico?v=<%= (1+Math.random())*1000%>" type="image/x-icon">
		<STYLE>
			.warningMessage { color:red; }
			body
			{
     			font-family: arial,verdana,helvetica;
     			font-size: 10pt;
			}
 
		</STYLE>

	</HEAD>


<frameset rows="20%,80%">
	<frame name="docheader" scrolling ="no"  src= "<%= request.getContextPath( )  %>/header.jsp"></frame>

	<frame name="docmenu" scrolling ="no"  src= "<%= request.getContextPath( )  %>/doc/"></frame>
</frameset>

<%@include file="/footer.jsp"%>