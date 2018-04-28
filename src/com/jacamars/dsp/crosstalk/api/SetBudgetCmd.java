package com.jacamars.dsp.crosstalk.api;

import com.jacamars.dsp.rtb.commands.SetPrice;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.AccountingCreative;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

/**
 * Wen API to set the price of a campaign/creative
 * @author Ben M. Faul
 *
 */
public class SetBudgetCmd extends ApiCommand {

	/** THe price to set.*/
	public double hourly;
	public double daily;
	public double total;

	/** If setting a deal price, then indicate that here */
	public String deal;

	/**
	 * Default constructor for the object.
	 */
	public SetBudgetCmd() {

	}

	/**
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public SetBudgetCmd(String username, String password) {
		super(username, password);
		type = SetBudget;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 * @param creative String. The target creative.
	 * @param hourly double. The hourly limit to set.
	 * @param daily double. The daily limit to set.
	 * @param total double. The total limit to set.
	 */
	public SetBudgetCmd(String username, String password, String campaign, String creative, double hourly, double daily, double total) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		this.daily = daily;
		this.hourly = hourly;
		this.total = total;
		type = SetBudget;
	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 */
	public SetBudgetCmd(String username, String password, String campaign, double hourly, double daily, double total) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		this.daily = daily;
		this.hourly = hourly;
		this.total = total;
		type = SetBudget;
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
		Statement stmt = config.getStatement();
		// String select =
		// "select * from Campaign where cost < total_budget and expire_time > ?
		// and activate_time <= ?";
		refreshList = null;

		if (Configuration.apiAcl.size() > 0 && Configuration.apiAcl.contains(campaign)) {
			error = true;
			message = "Error: campaign is not authorized to receive price modifications";
			return;
		}

		try {
			AccountingCampaign c = Scanner.getInstance().getCampaign(campaign);
			if (creative == null) {
				String update = "update campaigns set budget_limit_hourly = " + hourly +
						", budget_limit_daily = " + daily + ", total_budget = " + total + " where id = " + campaign;
				stmt.execute(update);
				if (c != null) {
					c.setTotalBudget(total);
					c.setDailyBudget(daily);
					c.setHourlyBudget(hourly);
				}

			} else {
				String table = "banners";
				for (AccountingCreative cc : c.getCreatives()) {
					if (cc.bannerid.equals(creative)) {
						if (cc.isVideo()) {
							table = "banner_videos";
						}
						String update = "update " + table + " set hourly_budget = " + hourly +
								", daily_budget = " + daily + ", total_basket_value = " + total + " where campaign_id = " + campaign
								+ " and id = " + creative;
						stmt.execute(update);
						cc.setTotalBudget(total);
						cc.setDailyBudget(daily);
						cc.setHourlyBudget(hourly);
					}
					error = true;
					message = "No such crative: " + creative + " in campaign: " + campaign;
					return;
				}

			}
		} catch (Exception e) {
			error = true;
			this.message = e.toString();
		}
	}
}
