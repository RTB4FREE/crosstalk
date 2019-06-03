package com.jacamars.dsp.crosstalk.api;

import com.jacamars.dsp.crosstalk.manager.Configuration;

/**
 * Web API to list all campaigns known by crosstalk
 * @author Ben M. Faul
 *
 */
public class ListSymbolsCmd extends ApiCommand {

	/** The list of symbols */
	public String id;

	/**
	 * Default constructor
	 */
	public ListSymbolsCmd() {

	}

	/**
	 * Deletes a campaign from the bidders.
	 *
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public ListSymbolsCmd(String username, String password) {
		super(username, password);
		type = ListSymbols;
	}

	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, msrshal the results.
	 */
	@Override
		public void execute() {
		super.execute();
		final Long id = random.nextLong();
		final ApiCommand theCommand = this;
		Thread thread = new Thread(new Runnable() {
			@Override
			public void run(){
				try {
					Configuration.getInstance().listSymbols(""+id);
				} catch (Exception e) {
					error = true;
					message = e.toString();
				}
			}
		});
		thread.start();
		asyncid = "" + id;
	}
}
