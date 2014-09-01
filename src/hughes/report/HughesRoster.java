/**
 * 
 */
package hughes.report;


import helios.api.report.frontend.ReportFrontEndGroups;
import helios.data.Datum;
import helios.database.connection.SQL.ConnectionFactory;
import helios.exceptions.DatabaseConnectionCreationException;
import helios.exceptions.ExceptionFormatter;
import helios.exceptions.ReportSetupException;
import helios.logging.LogIDFactory;
import helios.report.parameters.groups.ReportParameterGroups;
import helios.roster.Roster;
import helios.schedule.Scheduling;
import hughes.datasources.DatabaseConfigs;
import hughes.report.roster.Attributes;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;



/**
 * The roster containing the agents for whom we care about performance. This backend is not only used by the Roster report, but is used by other reports requiring 
 * an end-all list of users to report on. AgentName to CMS Name mappings are also loaded and can be referenced by implementing reports without additional database queries.
 * 
 * 
 * @author Jason Diamond
 *
 */
public final class HughesRoster extends Roster implements Attributes, Scheduling
{
	private final String dbPropFile = DatabaseConfigs.PRIVATE_LABEL_PROD_DB;
	private final static String PROGRAM_NAME = "Hughes";
	private final static String ORGUNIT_NAME = "NATS";
	
	private final static Logger logger = Logger.getLogger(HughesRoster.class);

	public static String uiGetReportName()
	{
		return "Hughes Roster";
	}
	
	public static String uiGetReportDesc()
	{
		return "A list of users in the roster.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.ROSTER_REPORT_PARAMETERS;
	
	/**
	 * Build the Roster report.
	 *
	 * @throws ReportSetupException	If a connection to the database could not be established.
	 */
	public HughesRoster() throws ReportSetupException 
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
			reportName = HughesRoster.uiGetReportName();
			reportDesc = HughesRoster.uiGetReportDesc();
			
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

	/**
	 * Close the report, any sub reports, and any database connections.
	 * 
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
	
	@Override
	public ArrayList<String> getReportSchema() 
	{
		ArrayList<String> retval = new ArrayList<String>();
		
		retval.add("User ID");
		retval.add("Login Name");
		retval.add("Teams");
		retval.add("First Name");
		retval.add("Last Name");
		retval.add("Extension");
		retval.add("Support Type ID");
		retval.add("Emp ID");
		retval.add("Email ID");
		retval.add("LMI Login Name");
		retval.add("LMI Node ID");
		retval.add("Full Name");
		retval.add("NTLogin");
		
		return retval;
	}

	/**
	 * Build the roster from the database. Also build the PARAM -> USER mappings for other reports to reference.
	 * 
	 */
	public void load()
	{
		logInfoMessage( "Loading Roster for likely a parent report: " + toString());

		clearUsers();

		String userID;
		String loginName;
		String teamName;
		String firstName;
		String lastName;
		String extension;
		String supportTypeID;
		String empID;
		String emailID;
		String lmiLogin;
		String lmiLoginNodeID;
		String fullName;
		String ntlogin;
		String active;

		Datum newUser = null;
		
		String query = 	"SELECT CRM_MST_USER.USER_USERID,CRM_MST_USER.USER_LOGINNAME,CRM_MST_USERTEAM.UTEAM_TEAMNAME,CRM_MST_USER.USER_FIRSTNAME, " +
				" CRM_MST_USER.USER_LASTNAME, CRM_MST_USER.USER_EXTENSION, CRM_MST_USER.USER_SUPPORTTYPEID, CRM_MST_USER.USER_EMPID, " +
				" CRM_MST_USER.USER_EMAILID, CRM_MST_USER.USER_LOGMEINLOGINNAME, CRM_MST_USER.USER_LOGMEINNODEID,CRM_MST_USER.USER_NTLOGINID,CRM_MST_USER.USER_RECORDSTATUS " +
				" FROM CRM_MST_USER INNER JOIN CRM_MST_USERTEAM ON CRM_MST_USER.USER_TEAMID = CRM_MST_USERTEAM.UTEAM_TEAMID " +
				" WHERE ( CRM_MST_USER.USER_SUPPORTTYPEID in ('151','159') AND CRM_MST_USER.USER_LOGINNAME NOT IN ('CHATUSER', 'WEBUSER', 'USER11') AND CRM_MST_USERTEAM.UTEAM_TEAMNAME NOT IN ('Management', 'SGS'))";
		
		for(String[] row:  dbConnection.runQuery(query))
		{
			try
			{					
				userID = row[0].trim();
				loginName = row[1].trim();
				teamName = row[2].trim();
				firstName = row[3].trim();
				lastName = row[4].trim();
				fullName = lastName + ", " + firstName;

				if(row[5] != null)
				{
					extension = row[5].trim();
				}
				else
				{
					extension="";
				}

				supportTypeID = row[6].trim();
				empID = row[7].trim().toUpperCase();
				emailID = row[8].trim();

				if(row[9] != null)
				{
					lmiLogin = row[9].trim();
				}
				else
				{
					lmiLogin ="";
				}

				if(row[10] != null)
				{
					lmiLoginNodeID = row[10].trim();
				}
				else
				{
					lmiLoginNodeID ="";
				}
				
				if(row[11] != null)
				{
					ntlogin = row[11].trim();
				}
				else
				{
					ntlogin ="";
				}
				
				if(row[12] != null)
				{
					active = row[12].trim();
				}
				else
				{
					active ="";
				}

				if( !hasUser(userID) ) 
				{
					newUser = new Datum(userID);

					newUser.addAttribute(USER_ID_ATTR);
					newUser.addAttribute(LOGIN_NAME_ATTR);
					newUser.addAttribute(TEAMNAME_ATTR);
					newUser.addAttribute(FIRSTNAME_ATTR);
					newUser.addAttribute(LASTNAME_ATTR);
					newUser.addAttribute(EXTENSION_ATTR); 
					newUser.addAttribute(SUPPORT_TYPE_ID_ATTR);
					newUser.addAttribute(EMP_ID_ATTR);
					newUser.addAttribute(EMAIL_ID_ATTR );
					newUser.addAttribute(LMI_LOGIN_NAME_ATTR); 
					newUser.addAttribute(LMI_LOGIN_NODE_ID_ATTR);
					newUser.addAttribute(FULLNAME_ATTR);
					newUser.addAttribute(NTLOGIN_ATTR);
					newUser.addAttribute(ACTIVE_ATTR);
					newUser.addAttribute(PROGRAMNAME_ATTR);
					newUser.addAttribute(ORGUNIT_ATTR);
					
					newUser.setAttributeAsUnique(USER_ID_ATTR);
					newUser.setAttributeAsUnique(NTLOGIN_ATTR);
					newUser.setAttributeAsUnique(EMAIL_ID_ATTR);
					newUser.setAttributeAsUnique(EMP_ID_ATTR);
					newUser.setAttributeAsUnique(EXTENSION_ATTR);
					newUser.setAttributeAsUnique(FULLNAME_ATTR);
					newUser.setAttributeAsUnique(LMI_LOGIN_NODE_ID_ATTR);

					newUser.addData(USER_ID_ATTR, userID);
					newUser.addData(LOGIN_NAME_ATTR, loginName);
					newUser.addData(TEAMNAME_ATTR, teamName);
					newUser.addData(FIRSTNAME_ATTR, firstName);
					newUser.addData(LASTNAME_ATTR, lastName);
					newUser.addData(EXTENSION_ATTR, extension);
					newUser.addData(SUPPORT_TYPE_ID_ATTR, supportTypeID);
					newUser.addData(EMP_ID_ATTR, empID);
					newUser.addData(EMAIL_ID_ATTR, emailID);
					newUser.addData(LMI_LOGIN_NAME_ATTR, lmiLogin);
					newUser.addData(LMI_LOGIN_NODE_ID_ATTR, lmiLoginNodeID);
					newUser.addData(FULLNAME_ATTR, fullName);
					newUser.addData(NTLOGIN_ATTR, ntlogin);
					newUser.addData(ACTIVE_ATTR, active);
					newUser.addData(PROGRAMNAME_ATTR, PROGRAM_NAME);
					newUser.addData(ORGUNIT_ATTR, ORGUNIT_NAME);
					
					
					
					if(shouldIncludeUser(newUser) || includeAllUsers)
					{
						addUser(userID, newUser);
					}
				}
			}
			catch(NullPointerException e)
			{
				logErrorMessage(  "Error adding user for line beginning with " + row[0]);
				logErrorMessage( ExceptionFormatter.asString(e));
			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}
		
		logInfoMessage( "Loaded " + getSize() + " users into roster");
	}
	
	/**
	 * Load the roster with Schedule data. This requires START_DATE_PARAM and END_DATE_PARAM to be defined for the Schedule subreport. This is going to wipe the existing roster by importing the userlist from it's child Schedule report, which maintains its own roster.
	 */
	public void loadSchedule()
	{		
//		Schedules s = null;
//		try 
//		{
//			s = new Schedules();
//
//			s.setParameters(getParameters());
//			s.setChildReport(true);
//			
//			//wipes the existing roster
//			clearUsers();
//			s.startReport();
//			for(String userID : s.getUserIDs())
//			{
//				addUser(userID, s.getUser(userID));
//			}
//		} 
//		catch (Exception e) 
//		{
//			logMessage(Level.ERROR,  "Error loading schedule: " + e.getMessage());
//			this.logException(e);
//		}
//		finally
//		{
//			if(s != null)
//			{
//				s.close();
//			}
//		}
	}

	/**
	 * Accessor for a specified User's name, for human readability.
	 * 
	 * @param userID	String to query the Users by. Can be Employee ID or User ID.
	 * 
	 * @return	The User discovered.
	 */
	public String getFullName(String userID)
	{
		Datum user = getUser(userID);

		String fullName = null;

		try
		{
			fullName = user.getAttributeData(LASTNAME_ATTR).get(0) + ", " + user.getAttributeData(FIRSTNAME_ATTR).get(0);
		}
		catch(NullPointerException e)
		{
			logErrorMessage( "Could not determine full name for parameter: " + userID);
		}

		return fullName;
	}

	/**
	 * Convert the userlist into something more user-readable.
	 * 
	 * @return	The roster. 
	 * @throws Exception 
	 * 
	 * @see report.Report#runReport()
	 */
	@Override
	protected ArrayList<String[]> runReport() throws Exception 
	{
		clearUsers();
		load();

		ArrayList<String[]> retval = new ArrayList<String[]>();
		Datum thisUser;
		for(String userID : getUserIDs())
		{
			thisUser = getUser(userID);
			
			retval.add
			(
					new String[]
					{
							thisUser.getAttributeData(USER_ID_ATTR).get(0),
							thisUser.getAttributeData(LOGIN_NAME_ATTR).get(0),
							thisUser.getAttributeData(TEAMNAME_ATTR).get(0),
							thisUser.getAttributeData(FIRSTNAME_ATTR).get(0),
							thisUser.getAttributeData(LASTNAME_ATTR).get(0),
							thisUser.getAttributeData(EXTENSION_ATTR).get(0),
							thisUser.getAttributeData(SUPPORT_TYPE_ID_ATTR).get(0),
							thisUser.getAttributeData(EMP_ID_ATTR).get(0),
							thisUser.getAttributeData(EMAIL_ID_ATTR).get(0),
							thisUser.getAttributeData(LMI_LOGIN_NAME_ATTR).get(0),
							thisUser.getAttributeData(LMI_LOGIN_NODE_ID_ATTR).get(0),
							thisUser.getAttributeData(FULLNAME_ATTR).get(0),
							thisUser.getAttributeData(NTLOGIN_ATTR).get(0)
					}
			);
		}
		
		return retval;
	}

	@Override
	public boolean isActiveUser(String userID) 
	{
		boolean retval = false;
		
		try
		{
			retval = getUser(userID).getAttributeData(ACTIVE_ATTR).get(0) == "1";
		}
		catch(Exception e)
		{}
		
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
