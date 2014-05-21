package hughes.api;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.TreeSet;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import hughes.report.HughesRoster;

public class GetTeamNames extends HttpServlet 
{
	private static final long serialVersionUID = 5393327027768241141L;

	private GetTeamNames() {}
	
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
			
			TreeSet<String> teamSet = new TreeSet<String>();
			
			for(String userID : roster.getUserIDs())
			{
				teamSet.addAll(roster.getTeamNames(userID));
			}
			
			out = response.getWriter();
			
			for(String teamName : teamSet)
			{
				out.println(teamName);
			}
		}
		catch (Exception t)
		{
			t.printStackTrace();
				
			//simple message for the user
			try 
			{
				out = response.getWriter();
				out.println("Error: " + t.getMessage());
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			finally
			{
				if(out != null)
				{
					out.close();
				}
			}
		}
		finally
		{
			if(out != null)
			{
				out.close();
			}
		}
	}
}
