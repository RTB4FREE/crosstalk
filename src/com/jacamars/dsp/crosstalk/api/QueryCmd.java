package com.jacamars.dsp.crosstalk.api;

import java.util.List;

import java.util.Random;

import com.jacamars.dsp.rtb.commands.ConfigureAwsObject;
import com.jacamars.dsp.rtb.commands.ConfigureObject;
import com.jacamars.dsp.rtb.commands.ListSymbols;
import com.jacamars.dsp.rtb.commands.QuerySymbol;
import com.jacamars.dsp.rtb.commands.SetPrice;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API access to query a symbol
 * etc.
 * 
 * @author Ben M. Faul
 *
 */
public class QueryCmd extends ApiCommand {

	/** The results of the command */
	public String updated;

	/** The command to execute */
	public String symbol;
	public String key;

	/**
	 * Default constructor
	 */

	public QueryCmd() {

	}

	/**
	 * Configures an File object.
	 * 
	 * @param username String. User authorization for command.
	 * @param password String. Password authorization for command.
	 */
	public QueryCmd(String username, String password) {
		super(username, password);
		type = QuerySymbol;
	}

	/**
	 * Targeted form of command. starts a specific bidder.
	 * 
	 * @param username String. User authorizatiom.
	 * @param password String. Password authorization.
	 * @param target   String. The bidder to start.
	 */
	public QueryCmd(String username, String password, String name, String key ) {
		super(username, password);
		this.symbol = symbol;
		this.key = key;
		type = QuerySymbol;
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
		final Long id = random.nextLong();
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					logger.debug("EXECUTING THE QUERY COMMAND: " + symbol + "/" + key);
					QuerySymbol sp = new QuerySymbol("", symbol, key);
					sp.from = WebAccess.uuid;
					sp.id = "" + id;
					sp.to = "*";
					Configuration.getInstance().sendCommand(true, sp);
				} catch (Exception err) {
					error = true;
					message = err.toString();
				}
			}
		});

		thread.start();
		asyncid = "" + id;
	}
}
