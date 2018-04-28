package com.jacamars.dsp.crosstalk.api;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import com.jacamars.dsp.rtb.commands.Echo;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Crosstalk;

/**
 * Web API to get the status of the bidders
 * @author Ben M. Faul
 *
 */
public class GetStatusCmd extends ApiCommand {

	/**
	 * Default constructor for the object.
	 */
	public GetStatusCmd() {

	}

	/** 
	 * Basic form of the command.
	 * @param username String. The username to use for authorization.
	 * @param password String. The password to use for authorization.
	 */
	public GetStatusCmd(String username, String password) {
		super(username, password);
		type = GetStatus;

	}

	/**
	 * Targeted form of the command.
	 * @param username String. The user authorization.
	 * @param password String. THe password authorization.
	 * @param target String. The target bidder.
	 */
	public GetStatusCmd(String username, String password, String target) {
		super(username, password);
		this.bidder = target;
		type = GetStatus;
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
	
		if (async == null || async == false) {
			commandStuff();
			return;
		}
		
		executeAsync();

	}
	
	void executeAsync() {
		final Long id = random.nextLong();
		final ApiCommand theCommand = this;
		Thread thread = new Thread(new Runnable() {
		    @Override
		    public void run(){
		    	try {
		    		commandStuff();
				} catch (Exception e) {
					error = true;
					message = e.toString();
				}
		    }
		});
		thread.start();
		asyncid = "" + id;
	}
	
	void commandStuff() {
		// Now wait for the response

		int i = 0;
		message = null;
		refreshList = new ArrayList();
		for (String bidder : Crosstalk.bidders) {
			Map m = Configuration.getInstance().redisson.hgetAll(bidder);
			if (this.bidder == null || this.bidder.equals(bidder)) {
				RefreshList item = new RefreshList(bidder);
				Map mx = Configuration.getInstance().redisson.hgetAll(bidder);
				if (mx == null) {
					logger.warn("Unexpected null returned for status of: {}", bidder);
					if (message == null)
						message = "Missing info for: " + bidder;
					else
						message += ", " + bidder;
					error = true;
				}
				else {
					item.campaigns = (ArrayList<String>)mx.get("campaigns");
					String test = (String)mx.get("stopped");
					if (test.equals("true"))
						item.stopped = true;
					else
						item.stopped = false;
					refreshList.add(item);
				}
			}
		}
	}
}
