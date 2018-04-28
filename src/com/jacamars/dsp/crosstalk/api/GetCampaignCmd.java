package com.jacamars.dsp.crosstalk.api;


import java.util.List;

import com.jacamars.dsp.rtb.common.Campaign;
import com.fasterxml.jackson.databind.JsonNode;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Get a campaign in JSON form of the base SQL object.
 * @author Ben M. Faul
 *
 */
public class GetCampaignCmd extends ApiCommand {

	/** The JSON node that represents the SQL of this campaign */
	public Campaign node;
	
	/**
	 * Default constructor
	 */
	public GetCampaignCmd() {

	}

	/**
	 * Basic form of the command..
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public GetCampaignCmd(String username, String password) {
		super(username, password);
		type = GetCampaign;
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
	public GetCampaignCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = GetCampaign;
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
				AccountingCampaign camp = Scanner.getInstance().getCampaign(campaign);
				if (camp == null) {
					error = true;
					message = "No such campaign";
				} else
					node = camp.campaign;
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
		}
}
