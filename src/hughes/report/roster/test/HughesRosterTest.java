/**
 * 
 */
package hughes.report.roster.test;

import helios.exceptions.ReportSetupException;
import helios.roster.attributes.BasicRosterAttributes;
import hughes.report.HughesRoster;
import hughes.report.test.ReportOutputTest;

import org.junit.Test;



/**
 * @author Jason Diamond
 *
 */
public class HughesRosterTest extends ReportOutputTest
{
	private HughesRoster roster;


	public void setUp()
	{

		try 
		{
			roster = new HughesRoster();

			
			
		} 
		catch (ReportSetupException e) 
		{
			assertFalse("Could not build HughesRoster report", true);

			e.printStackTrace();
		}
	}

	public void tearDown()
	{
		if(roster != null)
		{
			roster.close();
		}
	}
	
	@Test
	public void testUnfilteredRoster()
	{
		assertTrue( roster.startReport().size() == 0);
	}
	
	@Test
	public void testSingleTeamFilteredRoster()
	{
		String testTeam = "ROCJFS Sales Team";
		
		roster.getParameters().addTeamName(testTeam);
		
		roster.load();
		
		for(String userID : roster.getUserIDs())
		{
			assertTrue("Team membership of " + testTeam + " for user: " + roster.getUser(userID).toString(), roster.getUser(userID).getAttributeData(BasicRosterAttributes.TEAMNAME_ATTR).contains(testTeam));
		}
	}

	@Test
	public void testMultiTeamFilteredRoster()
	{
		String testTeam1 = "ROCJFS Sales Team";
		String testTeam2 = "Acer Voice - DLF - Kalpesh";
		
		roster.getParameters().addTeamName(testTeam1);
		roster.getParameters().addTeamName(testTeam2);
		
		roster.load();
		
		for(String userID : roster.getUserIDs())
		{
			assertTrue("Team membership of " + testTeam1 + " or " + testTeam2 + " for user: " + roster.getUser(userID).toString(), 
					roster.getUser(userID).getAttributeData(BasicRosterAttributes.TEAMNAME_ATTR).contains(testTeam1) ||
					roster.getUser(userID).getAttributeData(BasicRosterAttributes.TEAMNAME_ATTR).contains(testTeam2)
					);
		}
	}
	
	@Test
	public void testSingleUserFilteredRoster()
	{
		String agentName1 = "Beltz, Jason";
		
		roster.getParameters().addAgentName(agentName1);
		
		roster.load();
		
		assertTrue("Single user roster size", roster.getSize() == 1);
		
		String userID = roster.lookupUserByFullName(agentName1);
		
		assertNotNull("Agent name lookup for user: " + agentName1,userID);
		
		assertEquals("Team membership of " + agentName1 , agentName1, roster.getUser(userID).getAttributeData(BasicRosterAttributes.FULLNAME_ATTR).get(0));

	}
	
	@Test
	public void testMultiUserFilteredRoster()
	{
		String agentName1 = "Beltz, Jason";
		String agentName2 = "DeWert, Elizabeth";
		String agentName3 = "Vij, Nitin";
		
		roster.getParameters().addAgentName(agentName1);
		roster.getParameters().addAgentName(agentName2);
		roster.getParameters().addAgentName(agentName3);
		
		roster.load();
		
		assertTrue("Single user roster size", roster.getSize() == 3);
		
		String userID1 = roster.lookupUserByFullName(agentName1);
		String userID2 = roster.lookupUserByFullName(agentName2);
		String userID3 = roster.lookupUserByFullName(agentName3);
		
		assertNotNull("Agent name lookup for user: " + agentName1,userID1);
		assertNotNull("Agent name lookup for user: " + agentName2,userID2);
		assertNotNull("Agent name lookup for user: " + agentName3,userID3);
		
		assertEquals("Team membership of " + agentName1 , agentName1, roster.getUser(userID1).getAttributeData(BasicRosterAttributes.FULLNAME_ATTR).get(0));
		assertEquals("Team membership of " + agentName2 , agentName2, roster.getUser(userID2).getAttributeData(BasicRosterAttributes.FULLNAME_ATTR).get(0));
		assertEquals("Team membership of " + agentName3 , agentName3, roster.getUser(userID3).getAttributeData(BasicRosterAttributes.FULLNAME_ATTR).get(0));
	}
	
	@Test
	public void testLMITechIDResolution()
	{
		//xavier lobo
		//10586972
		String expectedAgentName = "Lobo, Xavier";
		String expectedLmiTechID = "10586972";
		String expectedUserID = "1342";
		
		roster.getParameters().addAgentName(expectedAgentName);
		roster.load();
		
		assertTrue("Single user roster size", roster.getSize() == 1);
			
		String actualUserID = roster.lookupUserByAttributeName(expectedLmiTechID, HughesRoster.LMI_LOGIN_NODE_ID_ATTR);
		
		assertNotNull("LMI Tech ID lookup for user: " + expectedAgentName, actualUserID);
		
		assertEquals("LMI Tech ID resolves to correct userID",expectedUserID,  actualUserID);
		
		assertEquals("LMI Tech ID lookup by username",expectedUserID,  roster.lookupUserByFullName(roster.getFullName(roster.lookupUserByAttributeName(expectedLmiTechID, HughesRoster.LMI_LOGIN_NODE_ID_ATTR)) ));
		
	}
}
