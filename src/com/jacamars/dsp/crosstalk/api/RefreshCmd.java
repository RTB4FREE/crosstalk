package com.jacamars.dsp.crosstalk.api;

import java.util.ArrayList;

import java.util.List;

import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API command that tells crosstalk to scan its SQL tables for updates.
 * 
 * @author Ben M. Faul
 *
 */
public class RefreshCmd extends ApiCommand {

	/** The returned campaigns that were scanned out of SQL */
	public List<String> updated;
	
	/** The campaigns to refresh, or if null, then all of them */
	public List<String> campaigns;

	/**
	 * Return the refreshed list of bidders/campaigns.
	 * 
	 * @return List. A list of bidders and their loaded campaigns.
	 */
	public List<RefreshList> getRefreshList() {
		return refreshList;
	}

	/**
	 * Set the list of bidder/campaigns.
	 * 
	 * @param refreshList
	 *            List. A list of refreshed campaigns/bidders.
	 */
	public void setBidders(List<RefreshList> refreshList) {
		this.refreshList = refreshList;
	}

	/**
	 * Basic form of the refresh command.
	 * 
	 * @param username
	 *            String. The username authorization.
	 * @param password
	 *            String. The password authorization.
	 */
	public RefreshCmd(String username, String password) {
		super(username, password);
		type = Refresh;
	}

	/**
	 * Default constructor
	 */
	public RefreshCmd() {

	}

	/**
	 * Return the JSON representation.
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the refresh and then return the campaigns that were loaded by
	 * crosstalk.
	 */
	@Override
	public void execute() {
		super.execute();

		try {	
			if (async == null || !async)
				updated = Scanner.getInstance().refresh();
			else {
				final Long id = random.nextLong();
				final ApiCommand theCommand = this;
				Thread thread = new Thread(new Runnable() {
				    @Override
				    public void run(){
				    	try {
							updated = Scanner.getInstance().refresh();
						} catch (Exception e) {
							error = true;
							message = e.toString();
						}
				    }
				});
				thread.start();
				asyncid = "" + id;
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
