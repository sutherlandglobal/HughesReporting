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
import com.sutherland.helios.data.granularity.user.UserGrains;
import com.sutherland.helios.database.connection.SQL.ConnectionFactory;
import com.sutherland.helios.database.connection.SQL.RemoteConnection;
import com.sutherland.helios.exceptions.DatabaseConnectionCreationException;
import com.sutherland.helios.exceptions.ExceptionFormatter;
import com.sutherland.helios.exceptions.ReportSetupException;
import com.sutherland.helios.logging.LogIDFactory;
import com.sutherland.helios.report.Report;
import com.sutherland.helios.report.parameters.groups.ReportParameterGroups;
import com.sutherland.helios.statistics.Statistics;
import com.sutherland.hughes.datasources.DatabaseConfigs;

/**
 * @author Jason Diamond
 *
 */
public final class RealtimeSalesQueue extends Report implements DataAttributes 
{
	private RemoteConnection dbConnection;
	private HughesRoster roster;

	private final static String dbPropFile = DatabaseConfigs.PRIVATE_LABEL_PROD_DB;
	private final static Logger logger = Logger.getLogger(RealtimeSalesQueue.class);
	

	public static String uiGetReportName()
	{
		return "Realtime Sales Queue";
	}
	
	public static String uiGetReportDesc()
	{
		return "Details of realtime sales orders.";
	}
	
	public final static LinkedHashMap<String, String> uiSupportedReportFrontEnds = ReportFrontEndGroups.STACK_RANK_FRONTENDS;
	
	public final static LinkedHashMap<String, ArrayList<String>> uiReportParameters = ReportParameterGroups.STACK_RANK_REPORT_PARAMETERS;
	
	/**
	 * Build the report object.
	 * 
	 * @throws ReportSetupException		If a failure occurs during creation of the report or its resources.
	 */
	public RealtimeSalesQueue() throws ReportSetupException
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
			reportName = RealtimeSalesQueue.uiGetReportName();
			reportDesc = RealtimeSalesQueue.uiGetReportDesc();
			
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
		
		String ordersQuery = 	"SELECT " +  
				" CRM_TRN_ORDER.ORDER_CREATEDBY," +
				" CRM_TRN_ORDERDETAILS.ORDDET_CREATEDDATE," +
				" CRM_TRN_ORDERDETAILS.ORDDET_AMOUNT," +
				" CRM_TRN_ORDER.ORDER_PROMOCODE," +	
				" CRM_TRN_ORDER.ORDER_OPTCOL7, " + //ordertype
				" CRM_TRN_ORDERDETAILS.ORDDET_SERVICETYPEID " +
				" FROM " +
				" CRM_TRN_ORDER inner join CRM_TRN_ORDERDETAILS on CRM_TRN_ORDER.ORDER_ORDERID = CRM_TRN_ORDERDETAILS.ORDDET_ORDERID "+
				" WHERE CRM_TRN_ORDER.ORDER_CREATEDDATE >= '" + 
				getParameters().getStartDate() +
				"' AND CRM_TRN_ORDER.ORDER_CREATEDDATE < '" + 
				getParameters().getEndDate() + 
				"' "; 
		
		retval = new ArrayList<String[]>();

		Aggregation reportGrainData = new Aggregation();

		String userID, reportGrain, orderAmount, promoCode, serviceTypeID;
		
		int userGrain;
		
		roster = new HughesRoster();
		roster.setChildReport(true);
		roster.getParameters().setAgentNames(getParameters().getAgentNames());
		roster.getParameters().setTeamNames(getParameters().getTeamNames());
		roster.load();
		
		//EZCLMSale sale;
		
		for(String[] row:  dbConnection.runQuery(ordersQuery))
		{
			userID = row[0];	

			if(roster.hasUser(userID) )
			{
				//orderDate = row[1];
				orderAmount = row[2];
				promoCode = row[3];
				serviceTypeID = row[5];
				
				if(serviceTypeID.equals("14"))
				{
					if(promoCode != null && promoCode.equals("RETSERVICE01"))
					{
						serviceTypeID = "Retention";
					}
					else
					{
						serviceTypeID = "Incident";
					}
				}
				else if(serviceTypeID.equals("15"))
				{
					serviceTypeID = "Subscription";
				}
				else
				{
					serviceTypeID = "Other";
				}
				
				
				userGrain = Integer.parseInt(getParameters().getUserGrain());
				reportGrain = UserGrains.getUserGrain(userGrain,roster.getUser(userID));
				
				reportGrainData.addDatum(reportGrain);
				reportGrainData.getDatum(reportGrain).addAttribute(serviceTypeID);
				reportGrainData.getDatum(reportGrain).addData(serviceTypeID, orderAmount);
				//reportGrainData.getDatum(reportGrain).addObject(EZCLM_SALE, sale);

			}
		}
		
		for( Entry<String, String> queryStats  : dbConnection.getStatistics().entrySet())
		{
			logInfoMessage( "Query " + queryStats.getKey() + ": " + queryStats.getValue());
		}

		//format the output

		retval = new ArrayList<String[]>();
		double totalAmount, totalAOV;
		int totalSales;
		for(String grain : reportGrainData.getDatumIDList())
		{
//			for ( Object realtimeSaleObj : reportGrainData.getDatum(grain).getDatumObjects(EZCLM_SALE))
//			{
//				sale = (EZCLMSale)realtimeSaleObj;
//			}
			
			//determine number of sales for each order type
			
			for(String thisServiceTypeID : reportGrainData.getDatum(grain).getAttributeNameList())
			{
				//add this user's entry for each service type
				totalAmount = Statistics.getTotal(reportGrainData.getDatum(grain).getAttributeData(thisServiceTypeID));
				totalSales = reportGrainData.getDatum(grain).getAttributeData(thisServiceTypeID).size();
				
				if(totalSales > 0)
				{
					totalAOV = totalAmount/totalSales;
				}
				else
				{
					totalAOV = 0;
				}
				
				
				retval.add(new String[]{grain, thisServiceTypeID, "" + totalSales, NumberFormatter.convertToCurrency(totalAmount), "" + NumberFormatter.convertToCurrency(totalAOV)});
			}
			
			
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
		
		//retval.add("Date Grain");
		
//		if(getParameters().getUserGrain().equals(UserGrains.AGENT_GRANULARITY))
//		{
//			retval.add("Team");
//		}
		
		retval.add("User Grain");
		
		//retval.add("Order Type");
		retval.add("Service Type");
		retval.add("# of Sales");
		retval.add("Total Sales ($)");
		retval.add("AOV ($)");
		
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
