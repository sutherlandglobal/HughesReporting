/**
 * 
 */
package hughes.report;


import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Aggregation;
import helios.data.attributes.DataAttributes;
import helios.data.granularity.time.TimeGrains;
import helios.data.granularity.user.UserGrains;
import helios.database.connection.SQL.ConnectionFactory;
import helios.database.connection.SQL.RemoteConnection;
import helios.date.parsing.DateParser;
import helios.exceptions.DatabaseConnectionCreationException;
import helios.exceptions.ExceptionFormatter;
import helios.exceptions.ReportSetupException;
import helios.formatting.NumberFormatter;
import helios.logging.LogIDFactory;
import helios.report.Report;
import helios.report.parameters.groups.ReportParameterGroups;
import helios.statistics.Statistics;
import hughes.datasources.DatabaseConfigs;

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
public final class RealtimeSales extends Report implements DataAttributes 
{
	private RemoteConnection dbConnection;
	private HughesRoster roster;
	private final String dbPropFile = DatabaseConfigs.PRIVATE_LABEL_PROD_DB;
	private final static Logger logger = Logger.getLogger(RealtimeSales.class);

	public static String uiGetReportName()
	{
		return "Realtime Sales";
	}
	
	public static String uiGetReportDesc()
	{
		return "Trends the total dollar value of sales.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public RealtimeSales() throws ReportSetupException
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
			reportName = RealtimeSales.uiGetReportName();
			reportDesc = RealtimeSales.uiGetReportDesc();
			
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
	 * @see helios.Report#runReport()
	 */
	@Override
	protected ArrayList<String[]> runReport() throws Exception
	{
		ArrayList<String[]> retval = null;

		String ordersQuery = "SELECT " +
				" CRM_MST_USER.USER_USERID,CRM_TRN_ORDERDETAILS.ORDDET_CREATEDDATE, " +
				" CRM_TRN_ORDERDETAILS.ORDDET_AMOUNT " + 
				" FROM CRM_MST_USER INNER JOIN CRM_TRN_ORDERDETAILS ON CRM_MST_USER.USER_USERID = CRM_TRN_ORDERDETAILS.ORDDET_CREATEDBY " + 
				" WHERE CRM_TRN_ORDERDETAILS.ORDDET_CREATEDDATE >= '" + 
				getParameters().getStartDate() +
				"' AND CRM_TRN_ORDERDETAILS.ORDDET_CREATEDDATE <= '" + 
				getParameters().getEndDate() + 
				"' "; 
		
		retval = new ArrayList<String[]>();

		Aggregation reportGrainData = new Aggregation();

		String userID, reportGrain, orderAmounts;
		
		//don't assign time grain just yet. in case this is a non-time report, because the timegrain param is not guaranteed to be set 
		int timeGrain, userGrain;
		
		roster = new HughesRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();
		
		for(String[] row:  dbConnection.runQuery(ordersQuery))
		{
			userID = row[0];	

			if(roster.hasUser(userID) )
			{
				orderAmounts = row[2];
				
				//time grain for time reports
				if(isTimeTrendReport())
				{
					timeGrain = Integer.parseInt(getParameters().getTimeGrain());
					reportGrain = TimeGrains.getDateGrain(timeGrain, DateParser.convertSQLDateToGregorian(row[1]));
				}
				else //if stack
				{
					userGrain = Integer.parseInt(getParameters().getUserGrain());
					reportGrain = UserGrains.getUserGrain(userGrain,roster.getUser(userID));
				}
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(SALES_AMTS_ATTR);
				reportGrainData.getDatum(reportGrain).addData(SALES_AMTS_ATTR, orderAmounts);
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}

		//format the output
		double finalSales;
		retval = new ArrayList<String[]>();
		for(String grain : reportGrainData.getDatumIDList())
		{
			finalSales = Statistics.getTotal(reportGrainData.getDatum(grain).getAttributeData(SALES_AMTS_ATTR));

			retval.add(new String[]{grain, NumberFormatter.convertToCurrency(finalSales) });
		}

		return retval;
	}

	/* (non-Javadoc)
	 * @see report.Report#close()
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
		
		retval.add("Sales ($)");
		
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
