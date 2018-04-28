package com.jacamars.dsp.crosstalk.config;

import com.jacamars.dsp.rtb.common.Configuration;

import java.util.List;

/**
 * Configuration for the application
 * @author Ben M. Faul
 *
 */
public class App {
	
	/** Identifies to Bidder who is sending the command */
	public String uuid;

	/** Web API port*/
	public String port = "6666";
	
	/** If specified, only these campaigns may receive api calls */
	public List<String> apiAcl;
	
	public App() {
		
	}

	public int getPort() throws Exception {
	    String str = Configuration.substitute(port);
	    if (str.startsWith("$"))
	        str = Configuration.GetEnvironmentVariable(str,str,"8100");
	    port = str;
	    return Integer.parseInt(str);
    }
}
