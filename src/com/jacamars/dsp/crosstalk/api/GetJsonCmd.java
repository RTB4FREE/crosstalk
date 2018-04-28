package com.jacamars.dsp.crosstalk.api;

import java.util.ArrayList;
import java.util.Random;

import com.jacamars.dsp.rtb.commands.Echo;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Crosstalk;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API to get the JSON of the campaign as RTB4FREE uses.
 * @author Ben M. Faul
 *
 */
public class GetJsonCmd extends ApiCommand {


	/** The RTB4FREE campaign object */
	public Campaign json;
	
	/**
	 * Default constructor for the object.
	 */
	public GetJsonCmd() {

	}

	/** 
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public GetJsonCmd(String username, String password) {
		super(username, password);
		type = GetJson;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param target String. The target bidder.
	 */
	public GetJsonCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = GetJson;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return mapper.writeValueAsString(this);
	}

	/**
	 * Executes the command, marshalls the response.
	 */
	@Override
	public void execute() {
		super.execute();
		AccountingCampaign c = Scanner.getInstance().getCampaign(campaign);
		if (c == null)
			c = Scanner.getInstance().deletedCampaigns.get(campaign);      // might be parked
		if (c == null) {
			error = true;
			message = "No such campaign: " + campaign;
		} else {
			json = c.campaign;
		}
		
	}
}
