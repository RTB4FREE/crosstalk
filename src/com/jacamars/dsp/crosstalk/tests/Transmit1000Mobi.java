package com.jacamars.dsp.crosstalk.tests;

import static org.junit.Assert.fail;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.ScannerIF;
import com.jacamars.dsp.rtb.bidder.ZPublisher;
import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.commands.PixelClickConvertLog;
import com.jacamars.dsp.rtb.fraud.FraudLog;
import com.jacamars.dsp.rtb.jmq.RTopic;
import com.jacamars.dsp.rtb.jmq.Publisher;
import com.jacamars.dsp.rtb.pojo.BidRequest;
import com.jacamars.dsp.rtb.pojo.BidResponse;
import com.jacamars.dsp.rtb.pojo.WinObject;

public class Transmit1000Mobi implements ScannerIF {
	
	public static void main(String ...args) throws Exception {
		new Transmit1000Mobi();
	}

	public Transmit1000Mobi() throws Exception {

		String crid = "9";
		
		WinObject obj = new WinObject();
		obj.pubId = "smartyads";
		obj.cost = "1000.0";
		obj.price = "1000.0";
		obj.adId  = "7";
		obj.cridId = crid;
		
		PixelClickConvertLog px = new PixelClickConvertLog();
		px.ad_id = "123";
		px.bid_id = "bid1000";
		px.creative_id = "crid123";
		px.exchange = "smartyads";
		px.type = PixelClickConvertLog.PIXEL;
		
		ZPublisher  wins = new ZPublisher("tcp://*:5572&wins");
	//	ZPublisher bids = new ZPublisher("tcp://*:5571","bids");
	//	ZPublisher clicks = new ZPublisher("tcp://*:5573&clicks");
		
		System.out.println("START");
		int k = 0;
		while(k < 1000) { // for (int i=0;i<10000;i++) {
			wins.add(obj);
		//	clicks.add(px);
			Thread.sleep(1);
			k++;
		}
		
		Thread.sleep(1000);
		System.out.println("\nA star is born!");
		System.exit(0);

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
	public void callBack(BasicCommand cmd) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}
}
