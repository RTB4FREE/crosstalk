package com.jacamars.dsp.crosstalk.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.jacamars.dsp.crosstalk.manager.Configuration;

/**
 * Get the assigned price of a Campaign/creative
 * @author Ben M. Faul
 *
 */
public class GetPriceCmd extends ApiCommand {
	
	public double price;
	public List<Map<String,Double>> deals;
	/**
	 * Default constructor for the object.
	 */
	
	public GetPriceCmd() {

	}

	/** 
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public GetPriceCmd(String username, String password) {
		super(username, password);
		type = GetPrice;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param campaign String. The target campaign.
	 * @param creative String. The target creative.
	 */
	public GetPriceCmd(String username, String password, String campaign, String creative) {
		super(username, password);
		this.campaign = campaign;
		this.creative = creative;
		type = GetPrice;
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
		price = 0;
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		// String select =
		// "select * from Campaign where cost < total_budget and expire_time > ?
		// and activate_time <= ?";
		refreshList = null;
		try {
			String select = "select * from banners where campaign_id = " + campaign  + " and id = " + creative;
			ResultSet rs = stmt.executeQuery(select);
			if (rs.next()) {
				price = rs.getDouble("bid_ecpm");
			} else {
				select = "select * from banner_videos where campaign_id = " + campaign  + " and id = " + creative;
				rs = stmt.executeQuery(select);
				if (rs.next()) {
					price = rs.getDouble("bid_ecpm");
					
					String spec = rs.getString("deals");
					if (!(spec == null || spec.trim().length() == 0)) {
						deals = new ArrayList();
						String[] parts = spec.split(",");
						for (String part : parts) {
							Map m = new HashMap();
							String[] subpart = part.split(":");
							String id = subpart[0].trim();
							Double price = Double.parseDouble(subpart[1].trim());
							m.put("id", id);
							m.put("price", price);
							deals.add(m);
						}
					}	
				} else {
					error = true;
					this.message = "No campaign/creative defined for " + campaign + "/" + creative;
				}
			}
		} catch (SQLException e) {
			error = true;
			this.message = e.toString();
		}
	}
}
