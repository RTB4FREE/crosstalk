package com.jacamars.dsp.crosstalk.api;


import java.util.List;

import java.util.Random;

import com.jacamars.dsp.rtb.commands.ConfigureAwsObject;
import com.jacamars.dsp.rtb.commands.SetPrice;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API access to configure an object from AWS S3, a bloom filter, etc.
 * @author Ben M. Faul
 *
 */
public class ConfigureAwsObjectCmd extends ApiCommand {

	/** The results of the command */
	public String updated;
	
	public String name;
	public String fileName;
	public String symbolType;
	/** The command to execute */
	String command;
	
	/**
	 * Default constructor
	 */

	public ConfigureAwsObjectCmd() {

	}

	/**
	 * Configures an S3 object.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public ConfigureAwsObjectCmd(String username, String password) {
		super(username, password);
		type = ConfigureAws;
	}

	/**
	 * Targeted form of command. starts a specific bidder.
	 * 
	 * @param username
	 *            String. User authorizatiom.
	 * @param password
	 *            String. Password authorization.
	 * @param target
	 *            String. The bidder to start.
	 */
	public ConfigureAwsObjectCmd(String username, String password, String name, String fileName, String symbolType) {
		super(username, password);
		this.name = name;
		this.fileName = fileName;
		this.symbolType = symbolType;
		type = ConfigureAws;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, msrshall the results.
	 */
	@Override
	public void execute() {
			super.execute();	
			try {
				command = "load S3 "+symbolType+" "+ name+" "+fileName;
				logger.debug("EXECUTING THE CONFIGURATION COMMAND: " + command);
				ConfigureAwsObject sp = new ConfigureAwsObject("","",command);
				sp.from = WebAccess.uuid + "-" + new Random().nextLong();
				Configuration.getInstance().sendCommand(true,sp);
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
	}
}
