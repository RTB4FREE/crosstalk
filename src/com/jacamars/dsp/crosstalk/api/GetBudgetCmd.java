package com.jacamars.dsp.crosstalk.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.jacamars.dsp.rtb.commands.Echo;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;

/**
 * Get the assigned budget for campaign/creatives
 * @author Ben M. Faul
 *
 */
public class GetBudgetCmd extends ApiCommand {

	/** The returned daily budget */
	public Double budget_limit_daily;  
	/** The returned budget limit hourly */
	public Double budget_limit_hourly;    
	/** returned total budget */
	public Double total_budget;                 
	
	/**
	 * Default constructor for the object.
	 */
	public GetBudgetCmd() {

	}

	/** 
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public GetBudgetCmd(String username, String password) {
		super(username, password);
		type = GetBudget;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 */
	public GetBudgetCmd(String username, String password, String campaign) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		type = GetBudget;
	}
	
	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 * @param creative String. The target creative.
	 */
	public GetBudgetCmd(String username, String password, String campaign, String creative) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		type = GetBudget;
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
		this.refreshList = null;
		
		if (creative == null)
			executeCampaignLevel();
		else
			executeCreativeLevel();
	}
		
	/**
	 * Execute at the campaign level
	 */
	void executeCampaignLevel() {
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		refreshList = null;
		
		try {
			String select = "select * from campaigns where id = " + campaign;;
			ResultSet rs = stmt.executeQuery(select);
			if (rs.next()) {
				 budget_limit_daily = rs.getDouble("budget_limit_daily");
				 budget_limit_hourly = rs.getDouble("budget_limit_hourly");
				 total_budget = rs.getDouble("total_budget");
			} else {
				error = true;
				this.message = "No campaign defined: " + campaign;
			}
		} catch (SQLException e) {
			error = true;
			this.message = e.toString();
		}
	}
	
	/**
	 * Execute at the creative level.
	 */
	void executeCreativeLevel() {
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		refreshList = null;
		
		try {
			String select = "select * from banners where campaign_id = " + campaign + " and id = " + creative;
			ResultSet rs = stmt.executeQuery(select);
			if (rs.next()) {
				 budget_limit_daily = rs.getDouble("daily_budget");
				 budget_limit_hourly = rs.getDouble("hourly_budget");
				 total_budget = rs.getDouble("total_basket_value");
			} else {
				select = "select * from banner_videos where campaign_id = " + campaign + " and id = " + creative;
				rs = stmt.executeQuery(select);
				if (rs.next()) {
					 budget_limit_daily = rs.getDouble("daily_budget");
					 budget_limit_hourly = rs.getDouble("hourly_budget");
					 total_budget = rs.getDouble("total_basket_value");
				} else {
					error = true;
					this.message = "No campaign/creative defined: " + campaign + "/" + creative;;
				}
			}
		} catch (SQLException e) {
			error = true;
			this.message = e.toString();
		}
	}
}
