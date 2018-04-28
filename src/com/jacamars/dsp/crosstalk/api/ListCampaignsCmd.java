package com.jacamars.dsp.crosstalk.api;


import java.util.ArrayList;
import java.util.List;

import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API to list all campaigns known by crosstalk
 * @author Ben M. Faul
 *
 */
public class ListCampaignsCmd extends ApiCommand {

	/** The list of campaigns */
	public List<String> campaigns;

	/**
	 * Default constructor
	 */
	public ListCampaignsCmd() {

	}

	/**
	 * Deletes a campaign from the bidders.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public ListCampaignsCmd(String username, String password) {
		super(username, password);
		type = ListCampaigns;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, msrshal the results.
	 */
	@Override
		public void execute() {
			super.execute();
			try {
				campaigns = new ArrayList<String>();
				for (AccountingCampaign c : Scanner.getInstance().campaigns) {
					campaigns.add(Integer.toString(c.campaignid));
				}
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
			error = true;
			if (message == null)
				message = "Timed out";
		}
}
