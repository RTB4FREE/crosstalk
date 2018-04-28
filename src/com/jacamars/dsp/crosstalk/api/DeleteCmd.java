package com.jacamars.dsp.crosstalk.api;


import java.util.List;

import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Deletes a campaign
 * @author Ben M. Faul
 *
 */
public class DeleteCmd extends ApiCommand {

	/** The list of deletions/updates */
	public List<String> updated;

	/**
	 * Default constructor
	 */
	public DeleteCmd() {

	}

	/**
	 * Deletes a campaign from the bidders.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public DeleteCmd(String username, String password) {
		super(username, password);
		type = Delete;
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
	public DeleteCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = Delete;
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
				updated = Scanner.getInstance().delete(campaign);
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
			message = "Timed out";
		}
}
