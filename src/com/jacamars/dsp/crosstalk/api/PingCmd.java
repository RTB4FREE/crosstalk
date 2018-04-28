package com.jacamars.dsp.crosstalk.api;

/**
 * Web API to send a ping command to crosstalk.
 * 
 * @author Ben M. Faul
 *
 */
public class PingCmd extends ApiCommand {
	
	/**
	 * Default constructor.
	 */
	public PingCmd() {
		
	}
	
	/** 
	 * Basic form of the command.
	 * @param username String. The username.
	 * @param password String. The password.
	 */
	public PingCmd(String username, String password) {
		super(username,password);
		type = Ping;
	}
	
	/**
	 * Convert to JSON
	 */
	@Override
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}
	
	/**
	 * Execute the command. Just updates its timestamp.
	 */
	@Override
	public void execute() {
		 super.execute();
		 refreshList = null;
	}
}
