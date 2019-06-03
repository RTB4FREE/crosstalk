package com.jacamars.dsp.crosstalk.api;

import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.rtb.commands.RemoveSymbol;

import java.util.Random;

/**
 * Web API access to configure an object from AWS S3, a bloom filter, etc.
 * @author Ben M. Faul
 *
 */
public class RemoveSymbolCmd extends ApiCommand {

	/** The results of the command */
	public String symbol;

	/**
	 * Default constructor
	 */

	public RemoveSymbolCmd() {

	}

	/**
	 * Configures an S3 object.
	 *
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public RemoveSymbolCmd(String username, String password) {
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
	public RemoveSymbolCmd(String username, String password, String target) {
		super(username, password);
		symbol = target;
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
				logger.debug("EXECUTING THE REMOVE SYMBOL: " + symbol);
				RemoveSymbol sp = new RemoveSymbol("","",symbol);
				sp.from = WebAccess.uuid + "-" + new Random().nextLong();
				Configuration.getInstance().sendCommand(true,sp);
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
	}
}
