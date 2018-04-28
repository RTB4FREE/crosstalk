package com.jacamars.dsp.crosstalk.api;

import java.util.ArrayList;
import java.util.List;

/**
 * A class that denotes bidder, status, and its campaigns.
 * @author Ben M. Faul
 *
 */
public class RefreshList {
 
	/**  The bidder name */
	public String bidder;
	
	/** Is the bidder stopped */
	public boolean stopped;
	
	/** The message from the bidder */
	public String message;
	
	/** A list of campaign id's in the bidder. */
	public List<String> campaigns;
	
	/**
	 * Default constructor
	 */
	public RefreshList() {
		
	}
	
	/**
	 * Constructor using the bidder id.
	 * @param bidder String. The instance name of the bidder.
	 */
	public RefreshList(String bidder) {
		this.bidder = bidder;
	}
	
	/**
	 * Add a campaign to the campaigns list.
	 * @param campaign String. The adid of the campaign to load.
	 */
	public void addCampaign(String campaign) {
		if (campaigns == null)
			campaigns = new ArrayList<String>();
		campaigns.add(campaign);
	}
}
