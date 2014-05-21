/**
 * 
 */
package hughes.report.test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import junit.framework.TestCase;

import org.junit.Test;

/**
 * @author Jason Diamond
 *
 */
public class ReportOutputTest extends TestCase 
{
	@Test
	public void testOutput(ArrayList<String[]> expected, ArrayList<String[]> actual)
	{
		if(actual != null)
		{
			if(expected.size() == actual.size())
			{


				//iterator practice
				Comparator<String[]> strArrayComp = 					
						new Comparator<String[] >()
						{
							@Override
							public int compare(String[] arg0, String[] arg1) 
							{
								//compare rows of strings as one long concat-ed string
								return Arrays.asList(arg0).toString().compareTo(Arrays.asList(arg1).toString());
							}
						};

				Collections.sort
				(
					expected,
					strArrayComp
				);

				Collections.sort
				(
					actual,
					strArrayComp
				);

						//			for(String[] row : actual)
						//			{
						//				System.out.println(Arrays.asList(row).toString());
						//			}
						//			
						//			System.out.println("=======================");
						//			
						//			for(String[] row : expected)
						//			{
						//				System.out.println(Arrays.asList(row).toString());
						//			}

				for(int i = 0; i< actual.size();i++)
				{
					//for(int j = 0; j< actual.get(i).length; j++)
					//{
						assertEquals
						(
							"Row comparison: " + 	Arrays.asList(expected.get(i)).toString() + " vs " + Arrays.asList(actual.get(i)).toString(), 
							Arrays.asList(expected.get(i)).toString(), 
							Arrays.asList(actual.get(i)).toString()
						);
					//}
				}
			}
			else
			{
				assertFalse("Output size mismatch. Expected: " + expected.size() + ", actual: " + actual.size() , true);
			}
		}
		else
		{
			assertFalse("Null output check", true);
		}
	}
}
