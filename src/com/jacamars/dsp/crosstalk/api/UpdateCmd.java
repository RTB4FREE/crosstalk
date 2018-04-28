package com.jacamars.dsp.crosstalk.api;

import java.util.HashMap;

import java.util.List;

import com.jacamars.dsp.crosstalk.manager.Scanner;

/**
 * Web API access for telling the bidders to update a campaign.
 * @author Ben M. Faul
 *
 */
public class UpdateCmd extends ApiCommand {

	/**
	 * Default constructor
	 */
	
	public List<String> updated;

	public UpdateCmd() {

	}

	/**
	 * Basic form of the command, update a named campaign.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public UpdateCmd(String username, String password) {
		super(username, password);
		type = Update;
	}

	/**
	 * Targeted form of command. Updates a specific campaign.
	 * 
	 * @param username
	 *            String. User authorizatiom.
	 * @param password
	 *            String. Password authorization.
	 * @param target
	 *            String. The bidder to start.
	 */
	public UpdateCmd(String username, String password, String target) {
		super(username, password);
		campaign = target;
		type = Update;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, marshal the results.
	 */
	@Override
		public void execute() {
			super.execute();	
			try {
				if (true /** async == null || !async */)              // force async
					message = Scanner.getInstance().update(campaign);
				else {
					final Long id = random.nextLong();
					final ApiCommand theCommand = this;
					Thread thread = new Thread(new Runnable() {
					    @Override
					    public void run(){
					    	try {
								message = Scanner.getInstance().update(campaign);
							} catch (Exception e) {
								error = true;
								message = e.toString();
								e.printStackTrace();
							}
					    }
					});
					thread.start();
					asyncid = ""+id;
				} 
			} catch (Exception err) {
				error = true;
				logger.error("Update command issued an error: " + err.toString());
				message = err.toString();
				err.printStackTrace();
			}
		}
}
