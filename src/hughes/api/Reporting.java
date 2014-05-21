/**
 * 
 */
package hughes.api;

import helios.api.format.output.CSVFormatter;
import helios.api.format.output.HTMLFormatter;
import helios.api.format.output.JSONFormatter;
import helios.api.format.output.XMLFormatter;
import helios.api.report.ReportProcessor;
import helios.api.report.frontend.ReportFrontEnds;
import helios.charting.BarChartFactory;
import helios.charting.StackedBarChartFactory;
import helios.charting.TimeLineChartFactory;
import helios.exceptions.ExceptionFormatter;
import helios.logging.LogIDFactory;
import helios.report.ReportTypes;
import helios.report.parameters.ParameterInfo;
import helios.report.parameters.ReportParameters;
import helios.report.parameters.parameter.Parameter;
import helios.report.parameters.sanitize.StringSanitizer;
import hughes.constants.Constants;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;
import org.jfree.chart.JFreeChart;

/**
 * @author Jason Diamond
 *
 */
public class Reporting extends HttpServlet 
{
	private static final long serialVersionUID = -6145013832591840295L;
	private final static Logger logger = Logger.getLogger(Reporting.class);
	private final static String LOG_ID_PREFIX = "api_id=";

	private Reporting(){}
	
	private void buildLogger()
	{
		String logID = LogIDFactory.getLogID().toString();

		if (MDC.get(LOG_ID_PREFIX ) == null) 
		{
			MDC.put(LOG_ID_PREFIX, LOG_ID_PREFIX + logID);
		}

	}
	
	@SuppressWarnings("unchecked")
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		//build an api logger to log stuff pulled from reportprocessor
		buildLogger();
		
		String fullRequestURL = request.getRequestURL().toString();
		String queryString = request.getQueryString();
		
		if(queryString != null)
		{
			fullRequestURL += "?" + queryString;
		}
		
		logger.log(Level.INFO, "Report request: " + fullRequestURL + " made by " + request.getRemoteUser() + " from " + request.getHeader("X-Forwarded-For") + " originating from " + request.getHeader("referer"));
		
		response.setCharacterEncoding("UTF-8");
		
		ReportProcessor reportProcessor = new ReportProcessor();

		reportProcessor.addJar(Constants.SITE_JAR);

		String reportName = null;

		ReportParameters rp = new ReportParameters();

		rp.addSupportedParameter(ParameterInfo.START_DATE_PARAM);
		rp.addSupportedParameter(ParameterInfo.END_DATE_PARAM);
		rp.addSupportedParameter(ParameterInfo.AGENT_NAMES_PARAM);
		rp.addSupportedParameter(ParameterInfo.TEAM_NAMES_PARAM);
		rp.addSupportedParameter(ParameterInfo.USER_GRAIN_PARAM);
		rp.addSupportedParameter(ParameterInfo.TIME_GRAIN_PARAM);
		rp.addSupportedParameter(ParameterInfo.NUM_DRIVERS_PARAM);
		rp.addSupportedParameter(ParameterInfo.REPORT_TYPE_PARAM);
		rp.addSupportedParameter(ParameterInfo.SOURCE_PARAM);

		ArrayList<String> apiParamNames = new ArrayList<String>();
		apiParamNames.add("reportName");
		apiParamNames.add("format");
		apiParamNames.add("delimiter");
		apiParamNames.add("enquote");
		apiParamNames.add("autoRefresh");

		ServletOutputStream out = null;
		PrintWriter writer = null;
		HashMap<String,String> requestParameters = new HashMap<String, String>();
		try
		{	
			long loadSiteStartTime = System.currentTimeMillis();
			reportProcessor.loadReportEntities();
			long loadSiteEndTime = System.currentTimeMillis();
			logger.log(Level.INFO, "Loaded Report Objects in: " +  (loadSiteEndTime - loadSiteStartTime) + " ms");

			String paramName;

			for (Map.Entry<String, String[]> requestParam : ((Map<String, String[]>)request.getParameterMap()).entrySet())
			{
				//skip the value cleaning for now, otherwise null becomes a string containing "null"
				paramName = requestParam.getKey();

				//param values can branch out multiple reports 
				//if there are multiple reports, only run the metric reports
				//max branches?

				for(String value : requestParam.getValue())
				{
					if(apiParamNames.contains(paramName))
					{
						requestParameters.put(paramName, value);
					}
					else
					{
						rp.addParameter(paramName, value);
					}
				}
			}

			//log report name and parameters, both api and report
			//maybe ip address et al.

			reportName = requestParameters.get(ParameterInfo.REPORT_NAME_HTTP_PARAM_NAME);
			logger.log(Level.INFO, "Building report: " + reportName);

			//tell site logs where we're coming from
			rp.addParameter(ParameterInfo.SOURCE_PARAM, "API");

			long reportStartTime = System.currentTimeMillis();
			ArrayList<String[]> results = reportProcessor.startReport(Constants.REPORT_CLASS_PREFIX + reportName, rp);
			long reportEndTime = System.currentTimeMillis();
			logger.log(Level.INFO, "Executed report in: " +  (reportEndTime - reportStartTime) + " ms");
			logger.log(Level.INFO, "Report returned rows: " + results.size());
			
			int reportFormat = Integer.parseInt(requestParameters.get("format"));

			switch(reportFormat)
			{
			case ReportFrontEnds.XML:
				writer = response.getWriter();
				
				XMLFormatter xmlFormatter = new XMLFormatter();
				response.setContentType("text/xml");
				response.setHeader("Content-type","application/xhtml+xml");
				
				for(String row : xmlFormatter.formatResults(results))
				{
					writer.println(row);
				}
				
				break;
			case ReportFrontEnds.HTML:
				
				
				writer = response.getWriter();
				
				
				//add html header

				HTMLFormatter htmlFormatter = new HTMLFormatter();

				String autoRefreshParam = requestParameters.get("autoRefresh");
				
				if(autoRefreshParam != null  )
				{
					htmlFormatter.setAutoRefresh("1".equals(StringSanitizer.sanitize(autoRefreshParam, Parameter.MAX_NAME_LEN)));
				}
				
				response.setContentType("text/html");

				for(String row : htmlFormatter.formatResults(reportProcessor.getReportSchema(), results))
				{
					writer.println(row);
				}

				//add html footer
				break;
			case ReportFrontEnds.JSON:
				JSONFormatter jsonFormatter = new JSONFormatter();
				writer = response.getWriter();

				response.setContentType("application/json");
				for(String row : jsonFormatter.formatResults(results))
				{
					writer.println(row);
				}
				break;
			case ReportFrontEnds.JFREECHART:
				//add html header

				int width = 1000, height = 600;
				//get report type so that the right chart can be built

				int reportType = Integer.parseInt(rp.getReportType());


				JFreeChart chart;
				ArrayList<String> schema = reportProcessor.getReportSchema();
				if(reportType == ReportTypes.TIME_TREND_REPORT)
				{
					if(!reportName.endsWith("Drivers"))
					{
						TimeLineChartFactory chartFactory = new TimeLineChartFactory(reportName, schema.get(0), schema.get(1), reportName);

						chart = chartFactory.buildChart(results, Integer.parseInt(rp.getTimeGrain()));
					}
					else
					{

						StackedBarChartFactory chartFactory = new StackedBarChartFactory(reportName, schema.get(0), schema.get(2));

						chart = chartFactory.buildChart(results);
					}
				}
				else //if(reportType == GetReportTypes.STACK_REPORT)
				{
					BarChartFactory chartFactory = new BarChartFactory(reportName, schema.get(0), schema.get(1), reportName);

					chart = chartFactory.buildChart(results);
				}

				BufferedImage buffImg = chart.createBufferedImage(width, height);
				ByteArrayOutputStream baos = null;

				try
				{
					baos = new ByteArrayOutputStream();
					ImageIO.write( buffImg, "png", baos );

					baos.flush();
					byte[] imageInByte = baos.toByteArray();
					baos.close();

					response.setContentType("image/png");

					out = response.getOutputStream();
					
					
					
					out.write(imageInByte);

				}
				catch (Exception e)
				{
					out.println("Error: " + e.getMessage());
					logger.log(Level.ERROR, ExceptionFormatter.asString(e));
				}
				finally
				{
					if(baos != null)
					{
						baos.close();
					}
				}

				//add html footer
				break;
			case ReportFrontEnds.CSV:
			default:
				writer = response.getWriter();
				CSVFormatter formatter = new CSVFormatter();

				String csvEnquoteParam = requestParameters.get("enquote");
				String csvDelimParam = requestParameters.get("delimiter");

				if(csvEnquoteParam != null  )
				{
					formatter.setEnquote("1".equals(StringSanitizer.sanitize(csvEnquoteParam, Parameter.MAX_NAME_LEN)));
				}

				if(csvDelimParam != null  )
				{
					formatter.setDelim(StringSanitizer.sanitize(csvDelimParam, Parameter.MAX_NAME_LEN));
				}
				
				response.setContentType("text/csv");

				for(String row : formatter.formatResults(results))
				{
					writer.println(row);
				}
				break;
			}
		}
		catch (Exception t)
		{
			try 
			{
				//simple message for the user
				//get error message from reportprocessor
				String errorMessage = reportProcessor.getErrorMessage();
				
				if(errorMessage == null || errorMessage.equals(""))
				{
					out.println("Error: " + t.getMessage());
				}
				
				logger.log(Level.ERROR, ExceptionFormatter.asString(t));
			} 
			catch (IOException e) 
			{
				logger.log(Level.ERROR, ExceptionFormatter.asString(e));
			}
			finally
			{
				if(out != null)
				{
					try 
					{
						out.close();
					} 
					catch (IOException e) 
					{
						logger.log(Level.ERROR, ExceptionFormatter.asString(e));
					}
				}
			}
		}
		finally
		{
			if(out != null)
			{
				try 
				{
					out.close();
				} 
				catch (IOException e) 
				{
					logger.log(Level.ERROR, ExceptionFormatter.asString(e));
				}
			}
			
			MDC.remove(LOG_ID_PREFIX);
		}
	}
}