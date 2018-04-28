package com.jacamars.dsp.crosstalk.config;

import com.jacamars.dsp.rtb.common.Configuration;

/**
 * Mysql configuration object
 * @author Ben M. Faul
 *
 */
public class Mysql {
	
	/** Sql Login string */
	public String defaultString = "jdbc:mysql://54.90.90.54/rtb4free?user=ben&password=whatweneednowislove";
	public String login;
	
	/**
	 * Default constructor
	 */
	public Mysql() {
		
	}

	public String getLogin() {
		if (login.startsWith("$")) {
			String name = login.substring(1);
			login = Configuration.GetEnvironmentVariable(login,"$JDBC",defaultString);
			System.out.println("*** JDBC: " + name + " has been substituted with " + login);
		}
		return login;
	}
}
