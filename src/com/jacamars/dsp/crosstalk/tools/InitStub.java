package com.jacamars.dsp.crosstalk.tools;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.ScannerIF;
import com.jacamars.dsp.rtb.commands.BasicCommand;

import com.jacamars.dsp.rtb.fraud.FraudLog;


public class InitStub implements ScannerIF {
	
	@Override
	public void callBack(BasicCommand msg) {

	}

	@Override
	public AccountingCampaign makeNewCampaign(ObjectNode node) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}
}