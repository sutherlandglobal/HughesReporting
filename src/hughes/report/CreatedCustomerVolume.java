/**
 * 
 */
package hughes.report;

import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Aggregation;
import helios.data.granularity.time.TimeGrains;
import helios.database.connection.SQL.ConnectionFactory;
import helios.database.connection.SQL.RemoteConnection;
import helios.date.parsing.DateParser;
import helios.exceptions.DatabaseConnectionCreationException;
import helios.exceptions.ExceptionFormatter;
import helios.exceptions.ReportSetupException;
import helios.logging.LogIDFactory;
import helios.report.Report;
import helios.report.parameters.groups.ReportParameterGroups;
import helios.report.parameters.validation.TimeIntervalValidator;
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
public final class CreatedCustomerVolume extends Report
{
	private RemoteConnection dbConnection;
	private final String dbPropFile = Constants.PRIVATE_LABEL_PROD_DB;
	private final static String CREATED_CUST_ATTR = "createdCustomers";
	private final static Logger logger = Logger.getLogger(CreatedCustomerVolume.class);
	
	public static String uiGetReportName()
	{
		return "Created Customers Volume";
	}
	
	public static String uiGetReportDesc()
	{
		return "Tally of created customers.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.BASIC_METRIC_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.BASIC_METRIC_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public CreatedCustomerVolume() throws ReportSetupException
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
			new LinkedHashMap<String,String>();
			
			reportName = CreatedCustomerVolume.uiGetReportName();
			reportDesc = CreatedCustomerVolume.uiGetReportDesc();
			
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
	protected ArrayList<String[]> runReport() 
	{
		ArrayList<String[]> retval = null;
		
		String query = "select " + 
				"CRM_MST_CONTACT.CONT_CREATEDDATE,"+
				"CRM_MST_CONTACT.CONT_CUSTOMERTYPE,"+
				"CRM_MST_CONTACT.CONT_HSNID,"+
				"ISNULL(CRM_MST_CONTACT.CONT_PROMOTIONCODE,''),"+
				"CRM_MST_CONTACT.CONT_MARKETINGCALLREASON,"+
				"CRM_MST_CONTACT.CONT_L1CENTER,"+
				"CRM_TRN_PROSPECT.PROSPECT_REASONFORNOSALE " +
				" from " + 
				" CRM_MST_CONTACT LEFT JOIN CRM_TRN_PROSPECT on CRM_MST_CONTACT.CONT_CONTACTID = CRM_TRN_PROSPECT.PROSPECT_CONTACTID " + 
				" where " + 
				"CRM_MST_CONTACT.CONT_CREATEDDATE >= '" + getParameters().getStartDate() +
				"' and CRM_MST_CONTACT.CONT_CREATEDDATE <= '" + getParameters().getEndDate() +"'";
		
		retval = new ArrayList<String[]>();
		
		int timeGrain;
		String reportGrain;
		
		Aggregation reportGrainData = new Aggregation();
		
		String creationDate, san;
		for(String[] row:  dbConnection.runQuery(query))
		{
			creationDate = row[0];
			san = row[2];
			
			//time grain for time reports
			if(isTimeTrendReport())
			{
				timeGrain = Integer.parseInt(getParameters().getTimeGrain());
				reportGrain = TimeGrains.getDateGrain(timeGrain, DateParser.convertSQLDateToGregorian(creationDate));
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(CREATED_CUST_ATTR);
				reportGrainData.getDatum(reportGrain).addData(CREATED_CUST_ATTR, san);
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}
		
		retval =  new ArrayList<String[]>();

		int customerCount;
		for(String grain : reportGrainData.getDatumIDList())
		{
			customerCount = 0;
			if( reportGrainData.getDatum(grain).getAttributeData(CREATED_CUST_ATTR) != null)
			{
				customerCount = reportGrainData.getDatum(grain).getAttributeData(CREATED_CUST_ATTR).size();
			}

			retval.add(new String[]{grain, "" + customerCount }) ;
		}
		
		return retval;
	}

	/* (non-Javadoc)
	 * @see report.Report#close()
	 */
	@Override
	public void close()
	{
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
	
	protected boolean validateParameters()
	{
		boolean retval = false;
		
		TimeIntervalValidator timeIntervalValidator = new TimeIntervalValidator();
		if(timeIntervalValidator.validate(this))
		{
			retval = true;
		}
		else
		{
			setErrorMessage(timeIntervalValidator.getErrorMessage());
		}
		return retval;
	}
	
	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();

		retval.add("Date");
		retval.add("Created Customer Count");
		
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
