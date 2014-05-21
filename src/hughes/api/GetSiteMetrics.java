/**
 * 
 */
package hughes.api;

import helios.api.report.ReportEntity;
import helios.api.report.ReportProcessor;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import hughes.constants.Constants;

/**
 * @author Jason Diamond
 *
 */
public class GetSiteMetrics extends HttpServlet 
{
	private static final long serialVersionUID = -2527779239979179254L;

	private GetSiteMetrics(){};
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		PrintWriter out = null;
		ReportProcessor reportProcessor = null;
		try
		{				
			reportProcessor = new ReportProcessor();
			reportProcessor.addJar(Constants.SITE_JAR);
			reportProcessor.loadReportEntities();
			
			out = response.getWriter();
			
			//name -> report class
			for(ReportEntity report : reportProcessor.getReportEntities())
			{
				out.println("\"" + report.getReportName() + "\",\"" + report.getReportClass() + "\""); 
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
