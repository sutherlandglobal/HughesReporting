package hughes.logging;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.LogManager;

public class Log4JCloser implements ServletContextListener
{
	@Override
	public void contextDestroyed(ServletContextEvent event) 
	{
		System.out.println("Executing Log4j shutdown");
		LogManager.shutdown();
	}

	@Override
	public void contextInitialized(ServletContextEvent event) 
	{
		
	}
}
