package com.jacamars.dsp.crosstalk.api;


import java.util.ArrayList;

import java.util.List;

import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API to return the reason a campaign is not loaded.
 * @author Ben M. Faul
 *
 */
public class GetReasonCmd extends ApiCommand {


	/** If only 1 reason, its here */
	public String reason;
	
	/** If there are more than one reason, they are here */
	public List<String> reasons;
	
	/**
	 * Default constructor
	 */
	public GetReasonCmd() {

	}

	/**
	 * Basic form of the command, starts all bidders.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public GetReasonCmd(String username, String password) {
		super(username, password);
		type = GetReason;
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
	public GetReasonCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = GetReason;
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
				if (campaign == null) {
					reasons = new ArrayList();
					for (AccountingCampaign camp : Scanner.getInstance().campaigns) {
						reasons.add(camp.campaignid + ": " + camp.report());
					}
					for (AccountingCampaign camp : Scanner.getInstance().getDeletedCampaigns()) {
						reasons.add(camp.campaignid + ": " + camp.report());
					}
					return;
				}
				AccountingCampaign camp = Scanner.getInstance().getCampaign(campaign);
				if (camp == null) 
						reason = "Unknown campaign";
					else
						reason = camp.report();
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
			message = "Timed out";
		}
}
