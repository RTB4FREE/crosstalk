package com.jacamars.dsp.crosstalk.api;

import java.sql.ResultSet;

import java.sql.SQLException;
import java.sql.Statement;

import com.jacamars.dsp.crosstalk.budget.Aggregator;
import com.jacamars.dsp.crosstalk.budget.BudgetController;
import com.jacamars.dsp.crosstalk.manager.Configuration;

/**
 * Web API to get the assigned budget for campaign/creatives
 * 
 * @author Ben M. Faul
 *
 */
public class GetValuesCmd extends ApiCommand {
	/** The daily budget value */
	public Double daily_value;

	/** The hourly budget value */
	public Double hourly_value;

	/** The total budget */
	public Double total_value;

	/** The adtype of the creative */
	public String adtype;

	/**
	 * Default constructor for the object.
	 */
	public GetValuesCmd() {

	}

	/**
	 * Basic form of the command.
	 * 
	 * @param username
	 *            String. The username to use for authorization.
	 * @param password
	 *            String. The password to use for authorization.
	 */
	public GetValuesCmd(String username, String password) {
		super(username, password);
		type = GetValues;

	}

	/**
	 * Targeted form of the command.
	 * 
	 * @param username
	 *            String. The user authorization.
	 * @param password
	 *            String. THe password authorization.
	 * @param campaign
	 *            String. The target campaign.
	 */
	public GetValuesCmd(String username, String password, String campaign) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		type = GetValues;
	}

	/**
	 * Targeted form of the command.
	 * 
	 * @param username
	 *            String. The user authorization.
	 * @param password
	 *            String. THe password authorization.
	 * @param campaign
	 *            String. The target campaign.
	 * @param creative
	 *            String. The target creative.
	 */
	public GetValuesCmd(String username, String password, String campaign, String creative) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		type = GetValues;
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
		refreshList = null;
		String type = null;
		try {
			if (creative == null) {
				total_value = BudgetController.getInstance().getCampaignTotalSpend(campaign);
				daily_value = BudgetController.getInstance().getCampaignDailySpend(campaign);
				hourly_value = BudgetController.getInstance().getCampaignHourlySpend(campaign);
			} else {
				if (adtype != null) {
					type = adtype;
				} else {
					if (Aggregator.creativeExists(campaign, creative, "banner"))
						type = "banner";
					if (Aggregator.creativeExists(campaign, creative, "video"))
						type = "video";
					if (Aggregator.creativeExists(campaign, creative, "native"))
						type = "native";
					if (Aggregator.creativeExists(campaign, creative, "audio"))
						type = "audio";
				}
				total_value = BudgetController.getInstance().getCreativeTotalSpend(campaign, creative, type);
				daily_value = BudgetController.getInstance().getCreativeDailySpend(campaign, creative, type);
				hourly_value = BudgetController.getInstance().getCreativeHourlySpend(campaign, creative, type);
			}
		} catch (Exception error) {
			this.error = true;
			message = error.toString();
		}
	}
}
