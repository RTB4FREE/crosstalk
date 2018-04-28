package com.jacamars.dsp.crosstalk.api;


import java.text.SimpleDateFormat;

import java.util.Date;

import com.jacamars.dsp.rtb.tools.HeapDumper;

/**
 * Dump heap to file.
 * @author Ben M. Faul
 *
 */
public class DumpCmd extends ApiCommand {

	/** The list of deletions/updates */

	/**
	 * Default constructor
	 */
	public DumpCmd() {

	}

	/**
	 * Dumps a heap to disk file.
	 * 
	 * @param username
	 *            String. User authorization for command.
	 * @param password
	 *            String. Password authorization for command.
	 */
	public DumpCmd(String username, String password) {
		super(username, password);
		type = Dump;
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
			try {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd:ss");
				String fileName = sdf.format(new Date()) + ".bin";
				message = "Dumped " + fileName;
				HeapDumper.dumpHeap(fileName, false);
				return;
			} catch (Exception err) {
				error = true;
				message = err.toString();
			}
			message = "Timed out";
		}
}
