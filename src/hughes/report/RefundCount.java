/**
 * 
 */
package hughes.report;


import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Aggregation;
import helios.data.granularity.time.TimeGrains;
import helios.data.granularity.user.UserGrains;
import helios.database.connection.SQL.ConnectionFactory;
import helios.database.connection.SQL.RemoteConnection;
import helios.date.parsing.DateParser;
import helios.exceptions.DatabaseConnectionCreationException;
import helios.exceptions.ExceptionFormatter;
import helios.exceptions.ReportSetupException;
import helios.logging.LogIDFactory;
import helios.report.Report;
import helios.report.parameters.groups.ReportParameterGroups;
import hughes.constants.Constants;

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
public class RefundCount extends Report
{
	private static final String REFUNDS_AMTS_ATTR = "refundAmounts";
	private RemoteConnection dbConnection;
	private HughesRoster roster;
	private final String dbPropFile = Constants.PRIVATE_LABEL_PROD_DB;
	private final static Logger logger = Logger.getLogger(RefundCount.class);

	public static String uiGetReportName()
	{
		return "Refund Count";
	}
	
	public static String uiGetReportDesc()
	{
		return "A count of refunds.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public RefundCount() throws ReportSetupException 
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
			reportName = RefundCount.uiGetReportName();
			reportDesc = RefundCount.uiGetReportDesc();
			
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
		boolean retval = false;

		try 
		{
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.load(dbPropFile);
			
			dbConnection = factory.getConnection();
		}
		catch(DatabaseConnectionCreationException e )
		{
			setErrorMessage("DatabaseConnectionCreationException on attempt to access database");
			
			logErrorMessage( getErrorMessage());
			logErrorMessage( ExceptionFormatter.asString(e));
		}
		finally
		{
			if(dbConnection != null)
			{
				retval = true;
			}
		}

		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport(java.lang.String, java.lang.String)
	 */
	@Override
	protected ArrayList<String[]> runReport() throws Exception
	{
		ArrayList<String[]> retval = null;
		
		String refundQuery = "SELECT CRM_TRN_REFUND.REFUND_CREATEDDATE,CRM_TRN_ORDER.ORDER_CREATEDBY,CRM_TRN_REFUND.REFUND_REFUNDAMOUNT " +
				" FROM CRM_TRN_REFUND INNER JOIN CRM_TRN_ORDER ON CRM_TRN_REFUND.REFUND_ORDERID = CRM_TRN_ORDER.ORDER_ORDERID " +  
				" WHERE CRM_TRN_REFUND.REFUND_CREATEDDATE >= '" + 
				getParameters().getStartDate() + 
				"' AND CRM_TRN_REFUND.REFUND_CREATEDDATE <= '" + 
				getParameters().getEndDate() +
				"' AND CRM_TRN_REFUND.REFUND_REFUNDTYPEID != 20000570 ";
		
		retval = new ArrayList<String[]>();

		Aggregation reportGrainData = new Aggregation();

		String userID, reportGrain, openedCaseID, rowDate;
		
		int timeGrain, userGrain;
		
		roster = new HughesRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();
		
		for(String[] row:  dbConnection.runQuery(refundQuery))
		{
			userID = row[1];
			
			if(roster.hasUser(userID) )
			{
				rowDate = row[0];
				openedCaseID = row[2];

				//time grain for time reports
				if(isTimeTrendReport())
				{
					timeGrain = Integer.parseInt(getParameters().getTimeGrain());
					reportGrain = TimeGrains.getDateGrain(timeGrain, DateParser.convertSQLDateToGregorian(rowDate));
				}
				else //if(isStackReport())
				{
					//is stack report
					userGrain = Integer.parseInt(getParameters().getUserGrain());
					reportGrain = UserGrains.getUserGrain(userGrain, roster.getUser(userID));
				}
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(REFUNDS_AMTS_ATTR);
				reportGrainData.getDatum(reportGrain).addData(REFUNDS_AMTS_ATTR, openedCaseID);
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}
		
		/////////////////
		//processing the buckets

		int numRefunds;
		
		retval = new ArrayList<String[]>(reportGrainData.getSize());
		
		for(String grain : reportGrainData.getDatumIDList())
		{
			numRefunds = reportGrainData.getDatum(grain).getAttributeData(REFUNDS_AMTS_ATTR).size();

			retval.add(new String[]{grain, "" + numRefunds });
		}

		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#runReport()
	 */
	@Override
	public void close()
	{
		if(roster != null)
		{
			roster.close();
		}

		if(dbConnection != null)
		{
			dbConnection.close();
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
		
		retval.add("Refund Count");
		
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
