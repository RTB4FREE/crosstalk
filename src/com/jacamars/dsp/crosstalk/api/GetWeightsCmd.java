package com.jacamars.dsp.crosstalk.api;

import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;
import com.jacamars.dsp.rtb.commands.GetWeights;


/**
 * Get the creative weights on this campaign.
 * @author Ben M. Faul
 *
 */
public class GetWeightsCmd extends ApiCommand {

	/**
	 * Default constructor for the object.
	 */

	public GetWeightsCmd() {

	}

	/**
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public GetWeightsCmd(String username, String password) {
		super(username, password);
		type = GetWeights;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 */
	public GetWeightsCmd(String username, String password, String campaign) {
		super(username, password);
		this.campaign = campaign;
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
		Configuration config = Configuration.getInstance();
		try {

			final String id = "" + random.nextLong();
            GetWeights wts = new GetWeights("*",campaign);
            asyncid = wts.id;
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run(){
					try {
					    AccountingCampaign c = Scanner.getActiveCampaign(campaign);
					    if (c == null) {
                            error = true;
                            message = "Campaign does not exist";
                            return;
                        }
						wts.from = WebAccess.uuid;
						config.sendCommand(true,wts);
					} catch (Exception e) {
						error = true;
						message = e.toString();
					}
				}
			});
			thread.start();
		} catch (Exception e) {
			error = true;
			this.message = e.toString();
		}
	}

}
