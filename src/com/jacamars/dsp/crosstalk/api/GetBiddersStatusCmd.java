package com.jacamars.dsp.crosstalk.api;


import java.util.ArrayList;

import java.util.List;
import java.util.Map;

import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Crosstalk;

/**
 * Get all the bidders status'es and put into a list
 * @author Ben M. Faul
 *
 */
public class GetBiddersStatusCmd extends ApiCommand {

	/** the list of bidders */
	public List<Map> entries;

	/**
	 * Default constructor
	 */
	public GetBiddersStatusCmd() {

	}

	/**
	 * Dumps a heap to disk file.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public GetBiddersStatusCmd(String username, String password) {
		super(username, password);
		type = GetBiddersStatus;
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
			entries = new ArrayList<Map>();
			try {
				for (String bidder : Crosstalk.bidders) {
					Map m = Configuration.getInstance().redisson.hgetAll(bidder);
					m.put("bidder", bidder);
					entries.add(m);
				}
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
		}
}
