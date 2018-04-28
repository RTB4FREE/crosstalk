package com.jacamars.dsp.crosstalk.manager;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ben on 7/17/17.
 */

public class CampaignBuilderWorker implements Runnable {

    /** Logging object */
    static final Logger logger = LoggerFactory.getLogger(CampaignBuilderWorker.class);
    private JsonNode jnode;
    private String msg;

    public CampaignBuilderWorker(JsonNode jnode){
    	this.jnode = jnode;
    }

    @Override
    public void run() {
        try {
        	ObjectNode node = (ObjectNode) jnode;
    		String campaign = jnode.get("id").asText();
    		List<String> bidders = new ArrayList<String>();

    		AccountingCampaign c = Scanner.getInstance().getKnownCampaign(campaign);

    		if (c == null && node == null) {
    			throw new Exception("Campaign is unknown: " + campaign);
    		}


    		// New campaign
    		if (c == null && node != null) {
    			c = Scanner.getInstance().makeNewCampaign(node);
				c.runUsingElk();
				if (c.campaignid == 1229) {
					System.out.println("here");
				}
    			if (c.isActive()) {
    				logger.info("New campaign {} going active",campaign);
    				msg = "NEW CAMPAIGN GOING ACTIVE: " + campaign;
    				Scanner.getInstance().campaigns.add(c);
    			} else {
    				logger.info("New campaign is inactive {}, reason: {}", campaign, c.report());
    				Scanner.getInstance().campaigns.remove(c);
    				Scanner.getInstance().deletedCampaigns.put(campaign, c);
    			}
    		} else if (node == null && c != null) {
    			logger.info("Deleting a campaign: {}",campaign);
    			msg = "DELETED CAMPAIGN: " + campaign;
    			c.report();
    			c.stop();
    			Scanner.getInstance().deletedCampaigns.put(campaign, c);
    			Scanner.getInstance().parkCampaign(c);
    		} else {
    			c.update(node);
    			if (c.isActive()) {
    				logger.info("Previously inactive campaign going active: {}",campaign);
    				if (Scanner.getInstance().deletedCampaigns.get(campaign) != null) {
    					Scanner.getInstance().deletedCampaigns.remove(campaign);
    					// campaigns.add(c);
    				}
    				msg = "CAMPAIGN GOING ACTIVE: " + campaign;
    				try {
    					c.addToRTB(); // notifies the bidder
    				} catch (Exception err) {
    					logger.error("Failed to load campaign {} into bidders, reason: {}", c.campaignid,err.toString());
    				}
    			} else {
    				logger.info("New campaign going inactive:{}, reason: {}", campaign,c.report());
    				msg = "CAMPAIGN GOING INACTIVE: " + campaign + ", reason: " + c.report();
    				Scanner.getInstance().parkCampaign(c); // notifies the bidder
    			}
    		}
        } catch (Exception error) {
        	error.printStackTrace();
            logger.error("Error creating campaign: {}", error.toString());
        }

    }

    @Override
    public String toString(){
        return msg;
    }
}
