/**
 * 
 */
package hughes.report;

import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Aggregation;
import helios.data.attributes.DataAttributes;
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
public final class NetSalesConversion extends Report implements DataAttributes 
{
	private CallVolume callVolumeReport;
	private NetSalesCount netSalesCountReport;
	private final static Logger logger = Logger.getLogger(NetSalesConversion.class);

	public static String uiGetReportName()
	{
		return "Net Sales Conversion";
	}
	
	public static String uiGetReportDesc()
	{
		return "Net percentage of calls where a sale has occured.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public NetSalesConversion() throws ReportSetupException
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
			reportName = NetSalesConversion.uiGetReportName();
			reportDesc = NetSalesConversion.uiGetReportDesc();
			
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
		if(callVolumeReport != null)
		{
			callVolumeReport.close();
		}
		
		if(netSalesCountReport != null)
		{
			netSalesCountReport.close();
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
		
		retval.add("Conversion (%)");
		
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

		netSalesCountReport = new NetSalesCount();
		netSalesCountReport.setChildReport(true);
		netSalesCountReport.setParameters(getParameters());

		callVolumeReport = new CallVolume();
		callVolumeReport.setChildReport(true);
		callVolumeReport.setParameters(getParameters());

		Aggregation reportGrainData = new Aggregation();
		String reportGrain;

		runner.addReport(CALL_VOL_ATTR, callVolumeReport);
		runner.addReport(SALES_COUNT_ATTR, netSalesCountReport);
		
		if(!runner.runReports())
		{
			throw new ReportSetupException("Running reports failed");
		}
		else
		{	
			for(String[] row : runner.getResults(CALL_VOL_ATTR))
			{
				reportGrain = row[0];

				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(CALL_VOL_ATTR );
				reportGrainData.getDatum(reportGrain).addData(CALL_VOL_ATTR, row[1]);
			}			

			/////////////////

			for(String[] row : runner.getResults(SALES_COUNT_ATTR))
			{
				reportGrain = row[0];
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(SALES_COUNT_ATTR );
				reportGrainData.getDatum(reportGrain).addData(SALES_COUNT_ATTR, row[1]);
			}
		}

		retval = new ArrayList<String[]>();
		
		double finalNumCalls, finalNumOrders, finalConversion;
		

		for(String datumID : reportGrainData.getDatumIDList())
		{
			finalConversion = 0;
			finalNumCalls = Statistics.getTotal(reportGrainData.getDatum(datumID).getAttributeData(CALL_VOL_ATTR));
			finalNumOrders = Statistics.getTotal(reportGrainData.getDatum(datumID).getAttributeData(SALES_COUNT_ATTR));
			
			if(finalNumCalls != 0)
			{
				finalConversion = finalNumOrders/finalNumCalls;
			}

			retval.add(new String[]{datumID, "" + NumberFormatter.convertToPercentage(finalConversion, 4) }) ;
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
