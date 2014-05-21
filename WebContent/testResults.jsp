<%@include file="/header.jsp" %>

<%@ page import="java.util.HashMap" %>
<%@ page import="java.io.File"%>
<%@ page import="java.io.BufferedReader"%>
<%@ page import="java.io.IOException"%>
<%@ page import="java.io.FileReader"%>
<%@ page import="java.util.Collections" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.ArrayList" %>
<%@ page import="uac.constants.Constants" %>
<%!
String testPrefix = "TEST-"; 
String testDir = Constants.PLATFORM_DIR + "/webapps/" + Constants.HELIOS_SITE_NAME+ "/test/";
String testCasePrefix = "Testcase: ";
String testRunPrefix = "Tests run: ";
%>

<%!

public String getTestNameFromFileName(String fileName)
{
	return fileName.substring(testPrefix.length(), fileName.length());
}

%>


<!-- for each file found, html up and add to hash -->

<%

HashMap<String,String> resultsHash = new HashMap<String,String>();
HashMap<String,Double> statsHash = new HashMap<String,Double>();

File dir = new File(testDir);

if(dir.exists())
{
	StringBuilder resultsHTML;
	StringBuilder testHTML;
	
	for(String fileName : dir.list())
	{
		if(fileName.startsWith(testPrefix))
		{
			resultsHTML = new StringBuilder();
			testHTML = new StringBuilder();
			
			String testName = getTestNameFromFileName(fileName);
			
			/*
			Testsuite: test.ConstantsTest
			Tests run: 4, Failures: 0, Errors: 0, Time elapsed: 0.008 sec

			Testcase: testGetInstance took 0.007 sec
			Testcase: testGetInstanceSingleton took 0 sec
			Testcase: testGetSuccess took 0.001 sec
			Testcase: testGetFailure took 0 sec
			*/
			
			BufferedReader dataIn = null;
			FileReader fin = null;
			
			try
			{
				fin = new FileReader(testDir + fileName);
				dataIn = new BufferedReader(fin);
				
				String line;
				while((line = dataIn.readLine()) != null)
				{
					if(line.equals(""))
					{
						testHTML.append("<ul>");
					}
					else if(line.startsWith(testCasePrefix))
					{
						testHTML.append("<li>");
						testHTML.append(line);
					}
					else
					{
						testHTML.append(line);
						testHTML.append("<br>");
						
						//global stats
						if(line.startsWith(testRunPrefix))
						{
							String[] fields = line.split("\\,");	
							
							String key;
							String val = null;
							for(String field: fields )
							{
								key = field.substring(0, field.indexOf(":")).trim();
								//out.println("key \"" +  key + "\"");
								try
								{
									val = field.substring(field.indexOf(":")+1);

									String secSuffix = " sec";
									
									if(val.endsWith(secSuffix))
									{
										val = val.substring(0, val.length() - secSuffix.length()).trim();
									}
									
									//out.println("val \"" + val + "\"");

									if(statsHash.containsKey(key))
									{
										statsHash.put(key, statsHash.get(key) + Double.parseDouble(val));
									}
									else
									{
										statsHash.put(key, Double.parseDouble(val));
									}
								}
								catch(NumberFormatException e)
								{
									e.printStackTrace();
								}
							}
						}
					}
					testHTML.append("\n");
					
					
				}
				testHTML.append("</ul>");
				
				resultsHash.put(testName, "<a name=\"" + testName + "\"><b>" + testName+ "</b></a><br>\n" + testHTML.toString() );
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
			finally
			{
				if(fin != null)
				{
					fin.close();
				}
				
				if(dataIn != null)
				{
					dataIn.close();
				}
			}
		}
	}
}


%>
<a name="home"></a>
<table width="100%" border ="1">
	<th>Test</th>
    <th>Results</th>
	<tr>
		<!--  column one -->
		<td width = "15%" valign="top">
						
		<% 
		
		out.println("<a href=\"#home\">Total</a><br>");
		
		List<String> sortedTestList = new ArrayList<String>(resultsHash.keySet());
		
		Collections.sort(sortedTestList);
		
		for(String test : sortedTestList)
		{
			out.println("<a href=\"#"+ test +"\">" + test + "</a><br>");
		}
		
		%>

		</td>
		<!--  column two -->
		<td width = "85%" valign="top" align="left">
		
		<%
		
		out.println("<b>Testing Totals: </b>");
		out.println("<ul>");
		
		try
		{
			out.println("<li>Tests Run: " + statsHash.get("Tests run").intValue());
			out.println("<li>Failures: " + statsHash.get("Failures").intValue());
			out.println("<li>Errors: " + statsHash.get("Errors").intValue());
			out.println("<li>Time elapsed: " + statsHash.get("Time elapsed") + " seconds");
		}
		catch(NullPointerException e)
		{
		
		}
		out.println("</ul>");
		
		for(String testHTML : sortedTestList)
		{
			out.println("<hr size=\"5\" color =\"blue\">");
			out.println(resultsHash.get(testHTML));
			out.println("<a href=\"#home\">Return to Top</a><br>");
		}
		out.println("<hr size=\"5\" color =\"blue\">");
		%>
		
		

		</td>
	</tr>
</table>


<%@include file="/footer.jsp"%>