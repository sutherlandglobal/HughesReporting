/**
 * 
 */
package com.sutherland.hughes.report;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

import com.sutherland.helios.api.report.frontend.ReportFrontEndGroups;
import com.sutherland.helios.data.Aggregation;
import com.sutherland.helios.data.attributes.DataAttributes;
import com.sutherland.helios.data.formatting.NumberFormatter;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.Report;
import com.sutherland.helios.report.ReportRunner;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.helios.statistics.Statistics;

/**
 * @author Jason Diamond
 *
 */
public final class HandleTime extends Report implements DataAttributes 
{
	private TalkTime talkTimeReport;
	private ACWTime acwTimeReport;
	private final static Logger logger = Logger.getLogger(HandleTime.class);

	public static String uiGetReportName()
	{
		return "Handle Time";
	}
	
	public static String uiGetReportDesc()
	{
		return "Handle Time.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public HandleTime() throws ReportSetupException
	{
		super();
	}
	
	/* (non-Javadoc)
	 * @see helios.Report#setupReport()
	 */
	@Override
	protected boolean setupReport() 
	{
		boolean retval = false;

		try
		{
			reportName = HandleTime.uiGetReportName();
			reportDesc = HandleTime.uiGetReportDesc();
			
			for(Entry<String, ArrayList<String>> reportType : uiReportParameters.entrySet())
			{
				for(String paramName :  reportType.getValue())
				{
					getParameters().addSupportedParameter(paramName);
				}
			}
			
			retval = true;
		}
		catch (Exception e)
		{
			setErrorMessage("Error setting up report");
			
			logErrorMessage( getErrorMessage());
			logErrorMessage( ExceptionFormatter.asString(e));
		}

		return retval;
	}
	
	@Override
	protected boolean setupLogger() 
	{
		logID = LogIDFactory.getLogID().toString();

		if (MDC.get(LOG_ID_PREFIX) == null) 
		{
			MDC.put(LOG_ID_PREFIX, LOG_ID_PREFIX + logID);
		}

		return (logger != null);
	}

	/* (non-Javadoc)
	 * @see helios.Report#setupDataSourceConnections()
	 */
	@Override
	protected boolean setupDataSourceConnections()
	{
		return true;
	}

	/* (non-Javadoc)
	 * @see report.Report#close()
	 */
	@Override
	public void close()
	{		
		if(talkTimeReport != null)
		{
			talkTimeReport.close();
		}
		
		if(acwTimeReport != null)
		{
			acwTimeReport.close();
		}

		super.close();
		
		if (!isChildReport) 
		{
			MDC.remove(LOG_ID_PREFIX);
		}
	}
	
	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();
		
		if(isTimeTrendReport())
		{
			retval.add("Date Grain");
		}
		else if(isStackReport())
		{
			retval.add("User Grain");
		}
		
		retval.add("Handle Time (Minutes)");
		
		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport()
	 */
	@Override
	protected ArrayList<String[]> runReport() throws ReportSetupException
	{
		//#calls fielded vs # orders made
		
		ArrayList<String[]> retval = null; 
		
		ReportRunner runner = new ReportRunner();

		talkTimeReport = new TalkTime();
		talkTimeReport.setChildReport(true);
		talkTimeReport.setParameters(getParameters());
		
		acwTimeReport = new ACWTime();
		acwTimeReport.setChildReport(true);
		acwTimeReport.setParameters(getParameters());



		Aggregation reportGrainData = new Aggregation();
		String reportGrain;


		runner.addReport(TALK_TIME_ATTR, talkTimeReport);
		runner.addReport(ACW_TIME_ATTR, acwTimeReport);
		
		if(!runner.runReports())
		{
			throw new ReportSetupException("Running reports failed");
		}
		else
		{	
			/////////////////

			for(String[] row : runner.getResults(TALK_TIME_ATTR))
			{
				reportGrain = row[0];
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(TALK_TIME_ATTR );
				reportGrainData.getDatum(reportGrain).addData(TALK_TIME_ATTR, row[1]);
			}
			
			for(String[] row : runner.getResults(ACW_TIME_ATTR))
			{
				reportGrain = row[0];
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(ACW_TIME_ATTR );
				reportGrainData.getDatum(reportGrain).addData(ACW_TIME_ATTR, row[1]);
			}
		}

		retval = new ArrayList<String[]>();
		
		double finalTalkTime, finalHandleTime, finalACWTime;
		

		for(String datumID : reportGrainData.getDatumIDList())
		{
			finalHandleTime = 0;
			finalTalkTime = Statistics.getTotal(reportGrainData.getDatum(datumID).getAttributeData(TALK_TIME_ATTR));
			finalACWTime = Statistics.getTotal(reportGrainData.getDatum(datumID).getAttributeData(ACW_TIME_ATTR));
			
			finalHandleTime = (finalTalkTime+finalACWTime);
			
			retval.add(new String[]{datumID, "" + NumberFormatter.convertToCurrency(finalHandleTime) }) ;
		}

		return retval;
	}
	
	@Override
	protected void logErrorMessage(String message) 
	{
		logger.log(Level.ERROR, message);
	}

	@Override
	protected void logInfoMessage(String message) 
	{
		logger.log(Level.INFO, message);
	}

	@Override
	protected void logWarnMessage(String message) 
	{
		logger.log(Level.WARN, message);
	}
}
