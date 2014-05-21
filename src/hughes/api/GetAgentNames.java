package hughes.api;

import hughes.report.HughesRoster;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.TreeSet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GetAgentNames extends HttpServlet 
{
	private static final long serialVersionUID = 5393327027768241141L;

	private GetAgentNames() {}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		HughesRoster roster = null;
		PrintWriter out = null;
		try
		{
			roster = new HughesRoster();
			//tell site logs where we're coming from
			roster.getParameters().setSource("UI");
			
			roster.setIncludeAllUsers(true);
			roster.load();
			
			TreeSet<String> agentSet = new TreeSet<String>();
			
			for(String userID : roster.getUserIDs())
			{
				agentSet.add(roster.getFullName(userID));
			}
			
			out = response.getWriter();
			
			for(String agentName : agentSet)
			{
				out.println(agentName);
			}
		}
		catch (Exception t)
		{
			t.printStackTrace();
				
			//simple message for the user
			try 
			{
				response.getWriter().println("Error: " + t.getMessage());
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
		}
		finally
		{
			if(roster != null)
			{
				roster.close();
			}
			
			if(out != null)
			{
				out.close();
			}
		}
	}
}
