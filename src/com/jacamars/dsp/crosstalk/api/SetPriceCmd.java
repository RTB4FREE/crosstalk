package com.jacamars.dsp.crosstalk.api;

import java.sql.ResultSet;


import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.commands.SetPrice;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Wen API to set the price of a campaign/creative
 * @author Ben M. Faul
 *
 */
public class SetPriceCmd extends ApiCommand {
	
	/** THe price to set.*/
	public double price;
	
	/** If setting a deal price, then indicate that here */
	public String deal;
	
	/**
	 * Default constructor for the object.
	 */
	public SetPriceCmd() {

	}

	/** 
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public SetPriceCmd(String username, String password) {
		super(username, password);
		type = SetPrice;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 * @param creative String. The target creative.
	 * @param price double. The price to set.
	 */
	public SetPriceCmd(String username, String password, String campaign, String creative, double price) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		this.price = price;
		type = SetPrice;
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
		double oldprice = 0;
		String oldDeals = null;
		String newDeals = null;
		
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		// String select =
		// "select * from Campaign where cost < total_budget and expire_time > ?
		// and activate_time <= ?";
		refreshList = null;
		
		if (Configuration.apiAcl.size()>0 && Configuration.apiAcl.contains(campaign)) {
			error = true;
			message = "Error: campaign is not authorized to receive price modifications";
			return;
		}
		
		try {
			String select = "select * from banners where campaign_id = " + campaign  + " and id = " + creative;
			ResultSet rs = stmt.executeQuery(select);
			if (rs.next()) {
				oldprice = rs.getDouble("bid_ecpm");
				if (deal != null) {
					oldDeals = rs.getString("deals");
					newDeals = convertDeals(oldDeals,deal,price);
					select = "update banners set deals = '" + newDeals + "'  where campaign_id = " + campaign + " and id = " + creative;
				} else
					select = "update banners set bid_ecpm = " + price + "  where campaign_id = " + campaign + " and id = " + creative;
				
			} else {
				select = "select * from banner_videos where campaign_id = " + campaign  + " and id = " + creative;
				rs = stmt.executeQuery(select);
				if (rs.next()) {
					oldprice = rs.getDouble("bid_ecpm");
					if (deal != null) {
						oldDeals = rs.getString("deals");
						newDeals = convertDeals(oldDeals,deal,price);
						select = "update banner_videos set deals = '" + newDeals + "' where campaign_id = " + campaign + " and id = " + creative;
					}
					else
						select = "update banner_videos set bid_ecpm = " + price + "  where campaign_id = " + campaign + " and id = " + creative;
				} else {
					error = true;
					this.message = "No campaign/creative defined for " + campaign + "/" + creative;
				}
			}
			
			if (error == null || error == false) {
				stmt.execute(select);
				SetPrice sp = new SetPrice("",campaign,creative,price);
				sp.from = WebAccess.uuid + "-" + new Random().nextLong();
				Configuration.getInstance().sendCommand(true,sp);
				
				AccountingCampaign camp = Scanner.getInstance().getCampaign(campaign);
				camp.updateSpike();
			}
		} catch (SQLException e) {
			error = true;
			this.message = e.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Given a String specification for deals: deal1:price,deal2:price,deal3:price, etc.
	 * @param spec String. The deal:price specifications.
	 * @param deal String. The deal price we are changing.
	 * @param nprice double. The new price.
	 * @return String. The new specification for the deals.
	 */
	String convertDeals(String spec, String deal, double nprice) {
		String[] parts = spec.split(",");
		String newString = "";
		for (String part : parts) {
			String[] subpart = part.split(":");
			String sid = subpart[0].trim();
			String price = subpart[1].trim();
			if (sid.equals(deal)) {
				price = "" + nprice;
			}
			newString += sid + ":" + price;
			newString += ",";
		}
		newString = newString.substring(0, newString.length()-1);
		return newString;
	}
}
