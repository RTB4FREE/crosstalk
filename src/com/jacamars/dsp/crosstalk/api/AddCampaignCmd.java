package com.jacamars.dsp.crosstalk.api;


import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API access to add  a campaign to the bidders.
 * @author Ben M. Faul
 *
 */
public class AddCampaignCmd extends ApiCommand {

	/** The returned results from the campaign. */
	public String updated;

	/**
	 * Default constructor
	 */
	public AddCampaignCmd() {

	}

	/**
	 * Deletes a campaign from the bidders.
	 *
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public AddCampaignCmd(String username, String password) {
		super(username, password);
		type = Add;
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
	public AddCampaignCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = Add;
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
				updated = Scanner.getInstance().add(campaign);
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
//			message = "Timed out";
		}
}
