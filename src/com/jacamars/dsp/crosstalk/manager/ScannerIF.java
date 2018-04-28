package com.jacamars.dsp.crosstalk.manager;

import com.fasterxml.jackson.databind.node.ObjectNode;

import com.jacamars.dsp.rtb.commands.BasicCommand;

import com.jacamars.dsp.rtb.fraud.FraudLog;


/**
 * Interface that Scanner's must implement. 
 * @author Ben M. Faul
 *
 */
public interface ScannerIF {

	public AccountingCampaign makeNewCampaign(ObjectNode node) throws Exception;
	public void start();
	public void callBack(BasicCommand cmd);
	
	public boolean isRunning();
}
