<%@ page import="java.util.LinkedHashMap" %>
<%@ page import="java.util.Map.Entry" %>

<hr size="5" color ="blue">
<table width="100%">
	<tr>
			<%
				String url = request.getRequestURL().toString();
				url = url.replace("//", "/");
			
				LinkedHashMap<String, String> navTable = new LinkedHashMap<String,String>();

				navTable.put("Home", request.getContextPath() + "/index.jsp");
				navTable.put("JUnit Testing", request.getContextPath() + "/testResults.jsp");
				navTable.put("Javadocs", request.getContextPath() + "/doc.jsp");				
				navTable.put("Logout", request.getContextPath() + "/logout.jsp");		

				for(Entry<String,String> sitePage : navTable.entrySet())
				{
					out.print("<td nowrap align=\"center\" valign=\"center\" width=\"" + 100/navTable.size() + "%\" " );
					
					
					if(url.endsWith(sitePage.getValue()))
					{
						out.println(" bgcolor=\"#bbbbbb\">");
					}
					else
					{
						out.println(" bgcolor=\"#ededed\">");
					}
					
					out.println("<a target=\"_top\" href=\"" + sitePage.getValue() + "\"><b>" + sitePage.getKey() + "</b></a>");
					
					out.println("</td>");
				}
			%>
	</tr>
</table>
<hr size="5" color ="blue">

