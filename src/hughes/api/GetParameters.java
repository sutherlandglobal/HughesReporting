package hughes.api;

import helios.api.report.ReportProcessor;
import helios.report.parameters.ParameterInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import hughes.constants.Constants;

/**
 * @author Jason Diamond
 *
 */
public class GetParameters extends HttpServlet 
{
	private static final long serialVersionUID = -3458625396879258440L;
	
	private GetParameters(){};
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		PrintWriter out = null;
		
		ReportProcessor reportProcessor = null;
		try
		{				
			if(request.getParameter(ParameterInfo.REPORT_NAME_HTTP_PARAM_NAME) != null && request.getParameter(ParameterInfo.REPORT_TYPE_HTTP_PARAM_NAME) != null )
			{
				String reportName = Constants.REPORT_CLASS_PREFIX + request.getParameter(ParameterInfo.REPORT_NAME_HTTP_PARAM_NAME);
				String reportType = request.getParameter(ParameterInfo.REPORT_TYPE_HTTP_PARAM_NAME);
				
				out = response.getWriter();
				reportProcessor = new ReportProcessor();
				reportProcessor.addJar(Constants.SITE_JAR);
				reportProcessor.loadReportEntities();

				out = response.getWriter();

				//report type -> list of parameters
				
				//ArrayList<String> paramNames = reportProcessor.getUIReportParameters(reportName).get(ReportTypes.TYPE_LOOKUP.get(reportType));
				ArrayList<String> paramNames = reportProcessor.getUIReportParameters(reportName).get(reportType);
				
				
				if(paramNames != null && paramNames.size() > 0)
				{
					paramNames.remove(ParameterInfo.REPORT_TYPE_PARAM);
					
					for(String paramName : paramNames)
					{
						out.print("\"" + paramName + "\",");
					}
					out.println();
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
