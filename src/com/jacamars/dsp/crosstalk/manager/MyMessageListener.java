package com.jacamars.dsp.crosstalk.manager;

import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.crosstalk.api.ApiCommand;
import com.jacamars.dsp.crosstalk.api.WebAccess;
import com.jacamars.dsp.rtb.bidder.Controller;
import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.jmq.MessageListener;

public class MyMessageListener implements MessageListener<BasicCommand>, Runnable {

	ObjectMapper mapper = new ObjectMapper();
	ScannerIF commandResponses;
	Logger logger;
	Thread me;
	public MyMessageListener(Logger logger, ScannerIF commandResponses) {
		this.logger = logger;
		this.commandResponses = commandResponses;
		me = new Thread(this);
		me.start();
	}
	
	public void run() {
		try {
			while(true)
				Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void onMessage(String channel, BasicCommand msg) {
		try {
			BidderProtocol.processAck(msg);
			if (msg.from.startsWith("crosstalk"))
				return;

			if (msg.cmd == Controller.SHUTDOWNNOTICE) {
				if (Crosstalk.tracker != null)
					Crosstalk.tracker.remove(msg.from);
				logger.warn("This bidder has sent a shutdown notice: {}", msg.from);
				return;
			}

			if (msg.to.startsWith(WebAccess.uuid)) {
				ApiCommand.callBack(msg);
				return;
			}

			if (Crosstalk.node != null)
				Crosstalk.node.addMember(msg.from);

			String content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(msg);
			// System.out.println("<------" + content);
			logger.debug(
					"Bidder(s) sent command response: {}, id: {}", msg.toString(), msg.id);


			if (msg.cmd == Controller.LIST_CAMPAIGNS_RESPONSE) {
				Configuration.getInstance().checkRunning(msg);
				return;
			}

			if (msg.cmd == Controller.ECHO) {
				int count = 0;
				while (Crosstalk.tracker == null && count < 15) {
					Thread.sleep(1000);
					count++;
				}
				if (Crosstalk.node == null) {
					logger.warn("Campaigns:node:error: node processor is down");
					return;
				}

				Crosstalk.tracker.checkFor(msg);
			}
			commandResponses.callBack(msg);
		} catch (Exception error) {
			error.printStackTrace();
		}
		
	}
}
