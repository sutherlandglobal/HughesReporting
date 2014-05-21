/**
 * 
 */
package hughes.api;

import helios.data.granularity.user.UserGrains;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * @author Jason Diamond
 *
 */
public class GetUserGrains extends HttpServlet 
{

	private static final long serialVersionUID = 2034751472577987571L;

	private GetUserGrains(){};
	
	public void doGet(HttpServletRequest request, HttpServletResponse response)
	{
		PrintWriter out = null;
		try
		{				
			out = response.getWriter();
			for(Entry<String, String> grain : UserGrains.avaliableUserGrains.entrySet())
			{
				out.println("\"" + grain.getKey() + "\",\"" + grain.getValue() + "\""); 
			}
		}
		catch (Exception t)
		{
			t.printStackTrace();
				
			//simple message for the user
			try 
			{
				out = response.getWriter();
				out.println("Error: " + t.getMessage());
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			}
			finally
			{
				if(out != null)
				{
					out.close();
				}
			}
		}
		finally
		{
			if(out != null)
			{
				out.close();
			}
		}
	}

}
