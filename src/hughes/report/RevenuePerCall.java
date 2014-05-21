/**
 * 
 */
package hughes.report;

import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Aggregation;
import helios.exceptions.ExceptionFormatter;
import helios.exceptions.ReportSetupException;
import helios.formatting.NumberFormatter;
import helios.logging.LogIDFactory;
import helios.report.Report;
import helios.report.ReportRunner;
import helios.report.parameters.groups.ReportParameterGroups;
import helios.statistics.Statistics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

/**
 * @author Jason Diamond
 *
 */
public final class RevenuePerCall extends Report 
{
	private final static String SALES_VAL_ATTR = "salesValues";
	private final static String CALL_VOL_ATTR = "callVol";
	private CallVolume callVolumeReport;
	private RealtimeSales realtimeSalesReport;
	private final static Logger logger = Logger.getLogger(RevenuePerCall.class);
	
	public static String uiGetReportName()
	{
		return "Revenue Per Call";
	}
	
	public static String uiGetReportDesc()
	{
		return "Sales dollars generated per call..";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public RevenuePerCall() throws ReportSetupException
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
			reportName = RevenuePerCall.uiGetReportName();
			reportDesc = RevenuePerCall.uiGetReportDesc();
			
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
		boolean retval = true;
		return retval;
	}

	/* (non-Javadoc)
	 * @see report.Report#close()
	 */
	@Override
	public void close()
	{
		if(callVolumeReport != null)
		{
			callVolumeReport.close();
		}
		
		if(realtimeSalesReport != null)
		{
			realtimeSalesReport.close();
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
		
		retval.add("RPC ($)");
		
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

		realtimeSalesReport = new RealtimeSales();
		realtimeSalesReport.setChildReport(true);
		realtimeSalesReport.setParameters(getParameters());

		callVolumeReport = new CallVolume();
		callVolumeReport.setChildReport(true);
		callVolumeReport.setParameters(getParameters());
		
		Aggregation reportGrainData = new Aggregation();

		String reportGrain, numCalls, salesTotal;

		runner.addReport(CALL_VOL_ATTR, callVolumeReport);
		runner.addReport(SALES_VAL_ATTR, realtimeSalesReport);
		
		if(!runner.runReports())
		{
			throw new ReportSetupException("Running reports failed");
		}
		else
		{	
			for(String[] row : runner.getResults(CALL_VOL_ATTR))
			{
				numCalls = row[1];
				reportGrain = row[0];

				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(CALL_VOL_ATTR);
				reportGrainData.getDatum(reportGrain).addData(CALL_VOL_ATTR, numCalls);
			}			

			/////////////////

			for(String[] row : runner.getResults(SALES_VAL_ATTR))
			{
				salesTotal = row[1];
				reportGrain = row[0];

				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(SALES_VAL_ATTR);
				reportGrainData.getDatum(reportGrain).addData(SALES_VAL_ATTR, salesTotal);
			}
		}

		retval = new ArrayList<String[]>();
		
		double finalNumCalls, finalSalesVal, finalRPC;

		for(String grain : reportGrainData.getDatumIDList())
		{
			finalRPC = 0;
			finalNumCalls = 0;
			finalSalesVal = 0;

			if(!reportGrainData.getDatum(grain).addAttribute(CALL_VOL_ATTR) && !reportGrainData.getDatum(grain).addAttribute(SALES_VAL_ATTR))
			{
				finalNumCalls = Statistics.getTotal(reportGrainData.getDatum(grain).getAttributeData(CALL_VOL_ATTR));
				finalSalesVal = Statistics.getTotal(reportGrainData.getDatum(grain).getAttributeData(SALES_VAL_ATTR));

				if(finalNumCalls != 0)
				{
					finalRPC = finalSalesVal/finalNumCalls;
				}
			}

			retval.add(new String[]{	grain,NumberFormatter.convertToCurrency(finalRPC)}	);
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
