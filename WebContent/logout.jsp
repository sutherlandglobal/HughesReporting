<%@include file="/header.jsp" %>
<%@ page import="java.util.Enumeration" %>

<script type="text/javascript" src="<%=request.getContextPath( )%>/js/jquery-1.10.2.min.js"></script>

<%@ page session="true"%>

User '<%=request.getRemoteUser()%>' has been logged out.
<br>
Please close your browser window to complete the logout process.

<%@include file="/footer.jsp"%>

<% 
session.invalidate(); 
request.logout();
%>