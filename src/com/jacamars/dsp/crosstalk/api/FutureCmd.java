package com.jacamars.dsp.crosstalk.api;

import com.jacamars.dsp.rtb.commands.BasicCommand;

/**
 * Retrieves the output of a stored future command.
 * @author Ben M. Faul
 *
 */
public class FutureCmd extends ApiCommand {

	/**
	 * Default constructor
	 */

	protected transient ApiCommand cmd;
	public Boolean completed;
	public FutureCmd() {

	}

	/**
	 * Basic form of the command, starts all bidders.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public FutureCmd(String username, String password) {
		super(username, password);
		type = Future;
	}


	/**
	 * Convert to JSON
	 */
	public String toJson() throws Exception {
		return WebAccess.mapper.writeValueAsString(this);
	}

	/**
	 * Execute the command, marshall the results.
	 */
	@Override
		public void execute() {
			BasicCommand c = ApiCommand.responses.get(asyncid);
			if (c != null) {
			    ApiCommand.responses.remove(asyncid);
                if(c.status.equalsIgnoreCase("ok")==false)
                    error = true;
                this.message = c.target;
			} else
			    this.message = "Nothing found";
		}
}