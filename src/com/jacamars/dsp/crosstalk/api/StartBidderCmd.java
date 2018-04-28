package com.jacamars.dsp.crosstalk.api;

import java.util.ArrayList;

import java.util.Random;

import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.commands.StartBidder;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Crosstalk;

/**
 * Web API for sending a command to Start bidders.
 * @author Ben M. Faul
 *
 */
public class StartBidderCmd extends ApiCommand {
	
	/**
	 * Default constructor
	 */
	public StartBidderCmd() {
		
	}
	
	/**
	 * Basic form of the command, starts all bidders.
	 * @param username String. User authorization for command.
	 * @param password String. Password authorization for command.
	 */
	public StartBidderCmd(String username, String password) {
		super(username,password);
		type = StartBidder;
	}
	
	/**
	 * Targeted form of command. starts a specific bidder.
	 * @param username String. User authorizatiom.
	 * @param password String. Password authorization.
	 * @param target String. The bidder to start.
	 */
	public StartBidderCmd(String username, String password, String target) {
		super(username,password);
		this.bidder = target;
		type = StartBidder;
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

		StartBidder e = null;

			try {
				e = new StartBidder();
				e.from = WebAccess.uuid + "-" + new Random().nextLong();
				if (bidder != null && !(bidder.length() == 0 || bidder.equals("*")))
					e.to = bidder;
				Configuration.getInstance().sendCommand(true, e);
			} catch (Exception error) {
				error.printStackTrace();
			}
			
			// Now wait for the response

			int i = 0;
			this.refreshList = new ArrayList<RefreshList>();
			String key = e.from;
			int bidderCount = 0;
			while(i<5000) {
				if (responses.get(key) != null) {
					BasicCommand bc = (BasicCommand) responses.get(e.from);
					RefreshList item = new RefreshList(bc.from);
					if (bc.status.equals("ok")) {
						item.stopped = false;
					} else {
						item.stopped = true;
					}
					item.message = bc.msg;
					refreshList.add(item);
					bidderCount++;
					if (bidder == null && bidderCount == Crosstalk.bidders.size()) {
						break;
					} else 
						break;
				}
				i+=500;
				try {
					Thread.sleep(500);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			error = true;
			message = "Timed out";
		}
}
