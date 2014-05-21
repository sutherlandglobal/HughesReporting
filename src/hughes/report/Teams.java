/**
 * 
 */
package hughes.report;


import helios.api.report.frontend.ReportFrontEndGroups;
import helios.database.connection.DatabaseConnection;
import helios.database.connection.SQL.ConnectionFactory;
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
import java.util.TreeSet;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;


/**
 * The list of teams within the Hughes support desk
 * 
 * @author Jason Diamond
 *
 */
public final class Teams extends Report 
{	
	private HughesRoster roster;
	private DatabaseConnection dbConnection;
	private final String dbPropFile = Constants.PRIVATE_LABEL_PROD_DB;
	private final static Logger logger = Logger.getLogger(Teams.class);

	public static String uiGetReportName()
	{
		return "Teams";
	}
	
	public static String uiGetReportDesc()
	{
		return "A list of teams in the roster.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.ROSTER_REPORT_PARAMETERS;
	
	/**
	 * Build the Roster report.
	 *
	 * @throws ReportSetupException	If a connection to the database could not be established.
	 */
	public Teams() throws ReportSetupException 
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
			reportName = Teams.uiGetReportName();
			reportDesc = Teams.uiGetReportDesc();
			
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

	/** 
	 * Attempt to establish connections to all required datasources. A report by definition has at least one, and possibly many.
	 * 
	 * @return	True if the connection was established, false otherwise.
	 */
	protected boolean setupDataSourceConnections()
	{
		boolean retval = false;

		try 
		{
			ConnectionFactory factory = new ConnectionFactory();
			
			factory.load(dbPropFile);
			
			dbConnection = factory.getConnection();
		}
		catch (DatabaseConnectionCreationException e) 
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

	/**
	 * Close the report, any sub reports, and any database connections.
	 * 
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

		retval.add("Team Name");
		retval.add("Desc");
		retval.add("Created Date");
		retval.add("Created By");
		
		return retval;
	}

	@Override
	protected ArrayList<String[]> runReport() throws Exception 
	{
		ArrayList<String[]> retval = new ArrayList<String[]>();

		roster = new HughesRoster();
		roster.setChildReport(true);
		roster.setParameters(getParameters());
		roster.load();

		//for each roster member, add every team we find to the list
		TreeSet<String> teamSet = new TreeSet<String>();
		for(String userID : roster.getUserIDs())
		{			
			teamSet.add(roster.getUser(userID).getAttributeData(HughesRoster.TEAMNAME_ATTR).get(0));
		}
		
		String[] teamCreatorRow;
		
		String teamCreator;
		for(String teamName : teamSet)
		{
			//for each teamname, query crm_mst_userteam table for more info
			
			for(String[] row : dbConnection.runQuery("Select uteam_description,uteam_createddate, uteam_createdby from crm_mst_userteam where uteam_teamname ='" + teamName + "'"))
			{
				if(row[2] != null)
				{
					try
					{
						//explicit query, since management creates a lot of users, and probably isn't in the roster
						teamCreatorRow = dbConnection.runQuery("select user_firstname, user_lastname from crm_mst_user where user_userid = '" + row[2] + "'").get(0);
						teamCreator = teamCreatorRow[0] + " " + teamCreatorRow[1];
					}
					catch(Exception e)
					{		
						logErrorMessage( ExceptionFormatter.asString(e));
						teamCreator = "";
					}
				}
				else
				{
					teamCreator = "";
				}
				
				retval.add(new String[]{teamName, row[0], row[1], teamCreator});
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}
	
		return retval;
	}

	/* (non-Javadoc)
	 * @see helios.Report#validateParameters()
	 */
	@Override
	public boolean validateParameters() 
	{
		return true;
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
