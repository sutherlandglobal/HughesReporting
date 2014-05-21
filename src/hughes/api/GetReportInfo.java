/**
 * 
 */
package hughes.api;

import java.io.IOException;
import java.io.PrintWriter;

import helios.api.report.ReportProcessor;
import helios.exceptions.ReportSetupException;
import helios.report.parameters.ParameterInfo;
import helios.report.parameters.parameter.Parameter;
import helios.report.parameters.sanitize.StringSanitizer;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import hughes.constants.Constants;

/**
 * @author Jason Diamond
 *
 */
public class GetReportInfo extends HttpServlet 
{
	private static final long serialVersionUID = 5245860999905884044L;

	private GetReportInfo() 
	{}
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		String reportName = null;
		PrintWriter out = null;
		
		ReportProcessor reportProcessor = null;
		try
		{	
			if(request.getParameter(ParameterInfo.REPORT_NAME_HTTP_PARAM_NAME) != null)
			{
				reportName = Constants.REPORT_CLASS_PREFIX + StringSanitizer.sanitize(request.getParameter(ParameterInfo.REPORT_NAME_HTTP_PARAM_NAME), Parameter.MAX_NAME_LEN);
				
				reportProcessor = new ReportProcessor();
				reportProcessor.addJar(Constants.SITE_JAR);
				reportProcessor.loadReportEntities();
				
				if( reportProcessor.isValidReportClassName(reportName))
				{
					out = response.getWriter();
					
					for(String info :  reportProcessor.getReportInfo(reportName))
					{
						out.println(info);
					}
				}
				else
				{
					throw new ReportSetupException("Invalid report selected");
				}
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
