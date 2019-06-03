package com.jacamars.dsp.crosstalk.api;


import java.util.List;

import java.util.Random;

import com.jacamars.dsp.rtb.commands.ConfigureAwsObject;
import com.jacamars.dsp.rtb.commands.ConfigureObject;
import com.jacamars.dsp.rtb.commands.SetPrice;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API access to configure an object from the file system, a bloom filter, etc.
 * @author Ben M. Faul
 *
 */
public class ConfigureObjectCmd extends ApiCommand {

	/** The results of the command */
	public String updated;
	
	/** The command to execute */
	public String name;
	public String fileName;
	public String fileType;
	
	/**
	 * Default constructor
	 */

	public ConfigureObjectCmd() {

	}

	/**
	 * Configures an File object.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public ConfigureObjectCmd(String username, String password) {
		super(username, password);
		type = Configure;
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
	public ConfigureObjectCmd(String username, String password, String name, String fileName, String fileType ) {
		super(username, password);
		this.name = name;
		this.fileName = name;
		this.fileType =  fileType;
		type = Configure;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, marshal the results.
	 */
	@Override
	public void execute() {
			super.execute();	
			try {
				logger.debug("EXECUTING THE FILE CONFIGURATION COMMAND: " + name + "/" + fileName + "/" + fileType );
				ConfigureObject sp = new ConfigureObject("",name,fileName,fileType);
				sp.from = WebAccess.uuid + "-" + new Random().nextLong();
				Configuration.getInstance().sendCommand(true,sp);
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
	}
}
