<%@include file="/header.jsp"%>

<%@ page import="helios.api.report.frontend.ReportFrontEnds"%>
<%@ page import="helios.report.ReportTypes"%>
<%@ page import="helios.data.granularity.time.TimeGrains"%>
<%@ page import="helios.data.granularity.user.UserGrains"%>
<%@ page import="helios.date.parsing.DateParser"%>
<%@ page import="helios.date.interval.Intervals"%>
<%@ page import="helios.report.parameters.ParameterInfo"%>
<%@ page import="hughes.constants.Constants"%>
<%@ page import="java.util.GregorianCalendar"%>
<%@ page import="java.util.ArrayList"%>
<%@ page import="java.util.HashMap"%>
<%@ page import="java.util.LinkedHashMap"%>
<%@ page import="java.util.Map.Entry"%>
<%@ page import="java.util.Arrays"%>

<!-- 
init helios ui component factory
	-request.getcontext path
get header (title)
	-local copy of favico
	-local copy of helios banner
get menu
	-hash of names -> urls
get footer
	-local copy of images
 -->

<script type="text/javascript"
	src="<%=request.getContextPath( )%>/js/jquery-1.10.2.min.js"></script>
<script type="text/javascript"
	src="<%=request.getContextPath( )%>/js/jquery.csv-0.71.min.js"></script>

<%
	GregorianCalendar now = new GregorianCalendar();
Intervals dateIntervals = new Intervals(now);


String nowParam = DateParser.toSQLDateFormat(now);

String todayStartParam = dateIntervals.getTimeInterval(Intervals.TODAY_INTERVAL_NAME).getStartDate();
String yesterdayStartParam = dateIntervals.getTimeInterval(Intervals.YESTERDAY_INTERVAL_NAME).getStartDate();
String yesterdayEndParam = dateIntervals.getTimeInterval(Intervals.YESTERDAY_INTERVAL_NAME).getEndDate();
String thisWeekStartParam = dateIntervals.getTimeInterval(Intervals.THIS_WEEK_INTERVAL_NAME).getStartDate();
String lastWeekStartParam = dateIntervals.getTimeInterval(Intervals.LAST_WEEK_INTERVAL_NAME).getStartDate();
String lastWeekEndParam = dateIntervals.getTimeInterval(Intervals.LAST_WEEK_INTERVAL_NAME).getEndDate();
String thisMonthStartParam = dateIntervals.getTimeInterval(Intervals.THIS_MONTH_INTERVAL_NAME).getStartDate();
String lastMonthStartParam = dateIntervals.getTimeInterval(Intervals.LAST_MONTH_INTERVAL_NAME).getStartDate();
String lastMonthEndParam = dateIntervals.getTimeInterval(Intervals.LAST_MONTH_INTERVAL_NAME).getEndDate();
String thisQuarterStartParam = dateIntervals.getTimeInterval(Intervals.THIS_QUARTER_INTERVAL_NAME).getStartDate();
String lastQuarterStartParam = dateIntervals.getTimeInterval(Intervals.LAST_QUARTER_INTERVAL_NAME).getStartDate();
String lastQuarterEndParam = dateIntervals.getTimeInterval(Intervals.LAST_QUARTER_INTERVAL_NAME).getEndDate();
String thisFiscalQuarterStartParam = dateIntervals.getTimeInterval(Intervals.THIS_FQ_INTERVAL_NAME).getStartDate();
String lastFiscalQuarterStartParam = dateIntervals.getTimeInterval(Intervals.LAST_FQ_INTERVAL_NAME).getStartDate();
String lastFiscalQuarterEndParam = dateIntervals.getTimeInterval(Intervals.LAST_FQ_INTERVAL_NAME).getEndDate();
String thisFiscalYearStartParam = dateIntervals.getTimeInterval(Intervals.THIS_FY_INTERVAL_NAME).getStartDate();
String lastFiscalYearStartParam = dateIntervals.getTimeInterval(Intervals.LAST_FY_INTERVAL_NAME).getStartDate();
String lastFiscalYearEndParam = dateIntervals.getTimeInterval(Intervals.LAST_FY_INTERVAL_NAME).getEndDate();
String thisYearStartParam = dateIntervals.getTimeInterval(Intervals.THIS_YEAR_INTERVAL_NAME).getStartDate();
String lastYearStartParam = dateIntervals.getTimeInterval(Intervals.LAST_YEAR_INTERVAL_NAME).getStartDate();
String lastYearEndParam = dateIntervals.getTimeInterval(Intervals.LAST_YEAR_INTERVAL_NAME).getEndDate();

//if agent time is selected, grab the roster type from the dropdown, and run a roster report through the api via xmlhttp
//if team time is selected, run the teams report through the api
%>

<table width="100%">
<tr>
<td align ="left" width="50%"><img src="<%= request.getContextPath() %>/images/site_banner.jpg" width="400" height="72"/></td>
<!-- <td align="right" width="20%"><b><%= Constants.HELIOS_READABLE_SITE_NAME %></b></td>-->
</tr>
</table>

<link rel="stylesheet" href="<%= request.getContextPath() %>/css/helios_site.css" type="text/css"  />

<!-- 
<table id="loadingSpace" width="100%">
<tr><td style="background-color: #ededed" align="center" valign="top">
<div id="loadingDiv" style="display: block;">
<img src="<%= request.getContextPath() %>/images/loading.gif" width="100" height="100" />
</div>
</td></tr>
</table>
-->

<div class="roundedtable">
<table id="mainTable">
	<thead>
		<tr>
			<th id="metricsHeader">Metrics</th>
			<th id="rosterHeader">Roster</th>
			<th id="paramsHeader">Parameters</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td id="metricsCol" align="left" valign="top" style="width: 175px;">
				<!--  report names -->
				<p><b>Select Metrics</b></p> 
				<select style="background-color: #ededed; border: 1px solid #0000FF; width: 175px;" id="metricSelectList" multiple></select>
			</td>
			<td id="rosterCol" align="left" valign="top" style="width: 400px" >
				<!-- user and team lists --> 
				
				<table id="rosterTable" style="width: 400px	; table-layout: fixed;" >
				<tr>
				<td valign="top">
					<p><b>Select Teams</b></p>
					<select style="background-color: #ededed; border: 1px solid #0000FF; width: 200px;" id="teamSelectList" multiple></select> 
				</td>
				<td valign="top">
					<p><b>Select Agents</b></p>
					<select style="background-color: #ededed; border: 1px solid #0000FF; width: 200px;" id="agentSelectList" multiple></select>
				</td>
				</tr>
				</table>
			</td>

			<td id="paramsCol" valign="top" align="left" style="width: 350px;">
				<table id="paramsTable" style="width: 350px; table-layout: fixed;">
					<tbody>
						<tr>
							<td><b>Select Parameters</b><br>
							<br></td>
						</tr>
						<tr id="selectMetricRow">
							<td>Select a Metric.</td>
						</tr>

						<tr id="reportInfoRow">
							<td>
								<p id="metricName">Metric Name</p>
								<p id="metricDesc">Metric Description</p> <br>
							</td>
						</tr>

						<tr id="reportTypesRow">
							<td><label id="reportTypesLabel" for="reportTypesSelectList">Choose
									a report type:&nbsp;</label> <!-- available report types --> <select
								id="reportTypesSelectList"></select></td>
						</tr>

						<tr id="dateIntervalsRow">
							<td><label id="dateIntervalLabel" for="dateIntervalsSelectList">Choose a date interval:</label>&nbsp;<select
								id="dateIntervalsSelectList">
									<option value="Today">Today</option>
									<option value="Yesterday">Yesterday</option>
									<option value="This Week">This Week</option>
									<option value="Last Week">Last Week</option>
									<option value="This Month">This Month</option>
									<option value="Last Month">Last Month</option>
									<option value="This Quarter">This Quarter</option>
									<option value="Last Quarter">Last Quarter</option>
									<option value="This Fiscal Quarter">This Fiscal
										Quarter</option>
									<option value="Last Fiscal Quarter">Last Fiscal
										Quarter</option>
									<option value="This Fiscal Year">This Fiscal Year</option>
									<option value="Last Fiscal Year">Last Fiscal Year</option>
									<option value="This Year">This Year</option>
									<option value="Last Year">Last Year</option>
							</select></td>
						</tr>

						<tr id="timeGrainsRow">
							<td><label id="timeGrainLabel" for="timeGrainsSelectList">Choose a time granularity:</label>&nbsp;<select id="timeGrainsSelectList"></select></td>
						</tr>

						<tr id="userGrainsRow">
							<td><label id="userGrainsLabel" for="userGrainsSelectList">Choose a user granularity:</label>&nbsp;<select id="userGrainsSelectList"></select></td>
						</tr>

						<tr id="numDriversRow">
							<td><label id="numDriversLabel" for="numDriversSelectList">Choose	number of drivers:</label>&nbsp;<select id="numDriversSelectList"></select></td>
						</tr>

						<tr id="reportFrontEndsRow">
							<td><label id="reportFrontEndLabel" for="reportFrontEndSelectList">Choose a format:</label>&nbsp;<select id="reportFrontEndsSelectList"></select></td>
						</tr>
						
						<tr id="autoRefreshRow">
							<td><label id="autoRefreshLebel" for="autoRefreshCheckBox">Auto Refresh:</label>&nbsp;<input type="checkbox" id="autoRefreshCheckBox" value="autoRefresh"></td>
						</tr>

						<tr id="reportLaunchButtonRow">
							<td>
								<button id="reportLaunchButton" type="button"
									onclick="launchReport()">Run!</button>
							</td>
						</tr>
					<tbody>
				</table>
			</td>

		</tr>
	</tbody>
</table>
</div>


<script type="text/javascript">

function httpGet(url)
{
    var xmlHttp = null;

    xmlHttp = new XMLHttpRequest();
    xmlHttp.open( "GET", url, false );
    xmlHttp.send( null );
    return xmlHttp.responseText;
}

function loadMetricList()
{
	$('#metricSelectList').html('');
	
	var siteMetrics = httpGet("<%=request.getContextPath()%>/siteMetrics").split("\n");
	
	for(var i = 0; i<siteMetrics.length; i++)
	{
		var metric = $.csv.toArray(siteMetrics[i]);
		
		if(metric.length == 2)
		{
			$("#metricSelectList").append("<option value=\"" + metric[0] +"\">" + metric[0] + "</option>");
		}
	}

	$('#metricSelectList').attr('size', 40);
}

function loadDateIntervals()
{
	
}

function loadTimeGrains()
{
	$('#timeGrainsSelectList').html('');
	
	var timeGrains = httpGet("<%=request.getContextPath()%>/timeGrains").split("\n");
	
	for(var i = 0; i<timeGrains.length; i++)
	{
		var timeGrain = $.csv.toArray(timeGrains[i]);
		
		if(timeGrain.length == 2)
		{
			$("#timeGrainsSelectList").append("<option value=\"" + timeGrain[1] +"\">" + timeGrain[0] + "</option>");
		}
	}
}

function loadUserGrains()
{
	$('#userGrainsSelectList').html('');
	
	var userGrains = httpGet("<%=request.getContextPath()%>/userGrains").split("\n");
	
	for(var i = 0; i<userGrains.length; i++)
	{
		var userGrain = $.csv.toArray(userGrains[i]);
		
		if(userGrain.length == 2)
		{
			$("#userGrainsSelectList").append("<option value=\"" + userGrain[1] +"\">" + userGrain[0] + "</option>");
		}
	}
}

function loadReportTypes(thisReport)
{
	//reset the list, otherwise we'll just keep appending
	$('#reportTypesSelectList').html('');
	
	//get the selected report types for the selected metric
	
	var reportTypes = httpGet("<%=request.getContextPath()%>/reportTypes?reportName=" + thisReport).split("\n");
		
	for(var i = 0; i<reportTypes.length; i++)
	{
		var reportType = $.csv.toArray(reportTypes[i]);
			
		if(reportType.length == 2)
		{
			$("#reportTypesSelectList").append("<option value=\"" + reportType[1] +"\">" + reportType[0] + "</option>");
		}
	}
}

function loadReportParameters(thisReport,reportType)
{
	//hide whatever params are already displayed
	$("#dateIntervalsRow").hide();
	$("#numDriversRow").hide();	
	$("#userGrainsRow").hide();	
	$("#timeGrainsRow").hide();
	$("#reportFrontEndsRow").hide();
	
	var paramString = httpGet("<%=request.getContextPath()%>/reportParameters?reportName=" + thisReport + "&reportType=" + reportType);
	var params = $.csv.toArray(paramString);
	
	for(var i = 0; i<params.length; i++)
	{
		if(params[i] == "<%=ParameterInfo.START_DATE_PARAM%>")
		{
			$("#dateIntervalsRow").show();
		}
		else if(params[i] == "<%=ParameterInfo.END_DATE_PARAM%>")
		{
			$("#dateIntervalsRow").show();
		}
		else if(params[i] == "<%=ParameterInfo.USER_GRAIN_PARAM%>")
		{
			$("#userGrainsRow").show();
		}
		else if(params[i] == "<%=ParameterInfo.TIME_GRAIN_PARAM%>")
		{
			$("#timeGrainsRow").show();
		}
		else if(params[i] == "<%=ParameterInfo.NUM_DRIVERS_PARAM%>")
		{
			$("#numDriversRow").show();
		}
	}
	
	loadSupportedFrontEnds(thisReport);
	
	$("#reportLaunchButtonRow").show();
}

function loadReportInfo(thisReport)
{
	//reset the list, otherwise we'll just keep appending
	$('#reportTypesSelectList').html('');
	
	//get the selected report types for the selected metric
	
	var reportInfoFields = httpGet("<%=request.getContextPath()%>/reportInfo?reportName=" + thisReport).split("\n");

	$("#metricName").html("<b>" + reportInfoFields[0] + "</b>");
	$("#metricDesc").html("<b>" + reportInfoFields[1] + "</b>");
}

function loadAgentNames()
{
	 $("#agentSelectList").html('');
	//web request to local roster report
	var agentNames = httpGet("<%=request.getContextPath()%>/agentNames").split("\n");
	
	for(var i = 0; i < agentNames.length; i++) 
	{
		if(agentNames[i] != "")
		{
	    	$("#agentSelectList").append("<option>" + agentNames[i] + "</option>");
		}
	}
	
	$('#agentSelectList').attr('size', 40);
}

function loadSupportedFrontEnds(thisReport)
{
	//load supported report front ends
	$("#reportFrontEndsSelectList").html('');
	var reportFrontEnds = httpGet("<%=request.getContextPath()%>/supportedFrontEnds?reportName=" + thisReport).split("\n");
	
	for(var i = 0; i<reportFrontEnds.length; i++)
	{
		var reportFrontEnd = $.csv.toArray(reportFrontEnds[i]);
		
		if(reportFrontEnd.length == 2)
		{
			$("#reportFrontEndsSelectList").append("<option value=\"" + reportFrontEnd[1] +"\">" + reportFrontEnd[0] + "</option>");
		}
	}
	
	$("#reportFrontEndsRow").show();
}

function loadTeamNames()
{
	 $("#teamSelectList").html('');
	//web request to local roster report
	var teamNames = httpGet("<%=request.getContextPath()%>/teamNames").split("\n");
	
	for(var i = 0; i < teamNames.length; i++) 
	{
		if(teamNames[i] != "")
		{
	    	$("#teamSelectList").append("<option>" + teamNames[i] + "</option>");
		}
	}
	
	$('#teamSelectList').attr('size', 40);
}

function loadNumDrivers()
{
	for(var i = 5; i <= 30; i+=5) 
	{
	    $("#numDriversSelectList").append("<option>" + i + "</option>");
	}
	
	//select default item
}

function loadStartTime()
{
	
}

function loadEndTime()
{
	
}

$( document ).ready(function() 
{
	$("#loadingSpace").show();
	$("#mainTable").hide();
	
	//hide until the lists are populated
	$("#metricSelectList").hide();
	$("#agentSelectList").hide();
	$("#teamSelectList").hide();
	
	$("#autoRefreshCheckBox").attr('checked', false);
	$("#autoRefreshRow").hide();
	
	$("#reportTypesRow").hide();
	 
	$("#dateIntervalsRow").hide();
	$("#numDriversRow").hide();	
	$("#userGrainsRow").hide();	
	$("#timeGrainsRow").hide();
	$("#reportFrontEndsRow").hide();
	$("#reportLaunchButtonRow").hide();
	
	$("#metricName").html('');
	$("#metricDesc").html('');
	
	//loading image for metric list
	//loading image for team list
	//loading image for agent list
	
	loadMetricList();
	loadNumDrivers();

	loadTeamNames();
	loadAgentNames();
	loadTimeGrains();
	loadUserGrains();

	$("#metricSelectList").show();
	$("#teamSelectList").show();
	$("#agentSelectList").show();
	$("#loadingSpace").hide();
	$("#mainTable").show();
});

$("#metricSelectList").change(function() 
{
	//get the selected values
	//alert( $("#metricSelectList").val() ); 
	
	var selectedReports = $("#metricSelectList").val() || [];
	
	if(selectedReports.length > 0)
	{
		$("#selectMetricRow").hide();
		
		var thisReport = selectedReports[0];
		
		//get this report's name and description. line 1 is name, line 2 is desc
		loadReportInfo(thisReport);
		
		//get this report's support types and load them
		loadReportTypes(thisReport);
		
		$("#reportTypesRow").show();
		$("#dateIntervalsRow").show();
		
		$("#autoRefreshCheckBox").attr('checked', false);
		$("#autoRefreshRow").hide();
		
		var selectedReportType = $("#reportTypesSelectList option:selected").text() || [];
		
		loadReportParameters(thisReport,selectedReportType);

	}
	else
	{
		$("#selectMetricRow").show();
		$("#metricName").html('');
		$("#metricDesc").html('');
		
		$("#autoRefreshCheckBox").attr('checked', false);
		$("#autoRefreshRow").hide();
		
		$("#reportTypesRow").hide();
		$("#dateIntervalsRow").hide();
		
		$("#timeGrainsRow").hide();
		$("#userGrainsRow").hide();
		$("#autoRefreshRow").hide();

		$("#numDriversRow").hide();	
		$("#reportFrontEndsRow").hide();
		$("#reportLaunchButtonRow").hide();
	}
});

$("#reportTypesSelectList").change(function() 
{
	var selectedReports = $("#metricSelectList").val() || [];
	
	if(selectedReports.length > 0)
	{
		var selectedReportType = $("#reportTypesSelectList option:selected").text() || [];
		loadReportParameters(selectedReports, selectedReportType);
	}
});

$("#reportFrontEndsSelectList").change(function() 
{
	//expose the autorefresh checkbox for html reports	
	//else, uncheck and hide
	
	var reportFrontEnd = $("#reportFrontEndsSelectList").val();
	
	if(reportFrontEnd == <%=ReportFrontEnds.HTML%>)
	{
		$("#autoRefreshRow").show();
	}
	else
	{
		$("#autoRefreshCheckBox").attr('checked', false);
		$("#autoRefreshRow").hide();
	}
	
});

//loadDateIntervals();
//loadTimeGrains();

function launchReport()
{
	var agentNames = $("#agentSelectList").val();
	
	var teamNames = $("#teamSelectList").val();
	
	//some reports will not have a roster, check to see if agentNames/teamNames are available
	if( (agentNames == null || agentNames.length == 0) && (teamNames == null || teamNames.length == 0))
	{
		teamNames =new Array();
		$("#teamSelectList option").each(function()
		{
			// add $(this).val() to your list
			teamNames.push($(this).val());
		});
	}

	
		//compile parameter url, switch on report type 
		
		var dateInterval =  $("#dateIntervalsSelectList").val();
		var timeGrain =  $("#timeGrainsSelectList").val();
		var userGrain =  $("#userGrainsSelectList").val();
		
		var reportURL ="<%=request.getContextPath()%>/";
		
		var reportType = $("#reportTypesSelectList").val();
		
		var selectedReports = $("#metricSelectList").val() || [];
		
		var reportFrontEnd = $("#reportFrontEndsSelectList").val();
		
		var numDrivers = $("#numDriversSelectList").val();
		
		if(selectedReports == null || selectedReports.length == 0)
		{
			alert("Please choose at least one metric.");
		}
		else 	if(reportType == null || reportType == "")
		{
			alert("Please choose a report type");
		}
		else 	if(reportFrontEnd == null || reportFrontEnd == "")
		{
			alert("Please choose a report format");
		}
		else 	if(dateInterval == null || dateInterval == "")
		{
			alert("Please choose a date interval");
		}
		else
		{
			var runReport = 1;
			
			//api source 
			reportURL += "reporting?reportName=" + selectedReports[0] + "&source=UI";
				
			reportURL += "&format=" + reportFrontEnd;
			reportURL += "&reportType=" + reportType;
			reportURL += "&numDrivers=" + numDrivers;
			
			if(reportFrontEnd == <%=ReportFrontEnds.CSV%>)
			{
				reportURL += "&enquote=1";
			}
			else if(reportFrontEnd == <%=ReportFrontEnds.HTML%>)
			{
				if($('#autoRefreshCheckBox').prop('checked') == true)
				{
					reportURL += "&autoRefresh=1";
				}
			}
					
			if(dateInterval == "Today")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=todayStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			else if(dateInterval == "Yesterday")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=yesterdayStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=yesterdayEndParam%>";
			}
			else if(dateInterval == "This Week")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisWeekStartParam%>";
				reportURL += "&endDate=";
				//reportURL += "<%=nowParam%>";
				reportURL += "now";
			}
			else if(dateInterval == "Last Week")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastWeekStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=lastWeekEndParam%>";
			}
			else if(dateInterval == "This Month")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisMonthStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
				
			}
			else if(dateInterval == "Last Month")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastMonthStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=lastMonthEndParam%>";
			}
			
			else if(dateInterval == "This Quarter")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisQuarterStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			else if(dateInterval == "Last Quarter")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastQuarterStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=lastQuarterEndParam%>";
			}
			
			else if(dateInterval == "This Fiscal Quarter")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisFiscalQuarterStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			else if(dateInterval == "Last Fiscal Quarter")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastFiscalQuarterStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=lastFiscalQuarterEndParam%>";
			}
			
			else if(dateInterval == "This Fiscal Year")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisFiscalYearStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			
			else if(dateInterval == "Last Fiscal Year")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastFiscalYearStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			
			else if(dateInterval == "This Year")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=thisYearStartParam%>";
				reportURL += "&endDate=";
				reportURL += "now";
			}
			else if(dateInterval == "Last Year")
			{
				reportURL += "&startDate="; 
				reportURL += "<%=lastYearStartParam%>";
				reportURL += "&endDate=";
				reportURL += "<%=lastYearEndParam%>";
			}
			
			if(reportType == <%=ReportTypes.TIME_TREND_REPORT%>)
			{
				if(timeGrain == null || timeGrain == "")
				{
					runReport =0;
					alert("Please choose a time granularity");
				}
				else
				{
					reportURL += "&timeGrain=" + timeGrain; 
				}
			}
			else if(reportType == <%=ReportTypes.STACK_REPORT%>)
			{
				if(userGrain == null || userGrain == "")
				{
					runReport = 0;
					alert("Please choose a user granularity");
				}
				else 
				{
					reportURL += "&userGrain=" + userGrain; 
				}
			}
			
			if(agentNames != null)
			{
				for(var i =0; i<agentNames.length; i++)
				{
					if(agentNames[i] != null)
					{
						reportURL += "&agentName=" + agentNames[i];
					}
				}
			}
			
			if(teamNames != null)
			{
				for(var i =0; i<teamNames.length; i++)
				{
					if(teamNames[i] != null)
					{
						reportURL += "&teamName="+teamNames[i];
					}
				}
			}
			
			//launch report
			
			if(runReport == 1)
			{
				window.open(reportURL);
			}
		
	}
}

</script>


<%@include file="/footer.jsp"%>
