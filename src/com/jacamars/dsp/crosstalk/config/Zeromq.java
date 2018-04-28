package com.jacamars.dsp.crosstalk.config;

import java.util.List;

/**
 * 0MQ configuration object.
 * @author Ben M. Faul
 *
 */
public class Zeromq {

	/** The request channel */
	public String requestchannel;
	
	/** The bid channel */
	public String bidchannel;
	
	/** The win channel */
	public String winchannel;
	
	/** The clicks channel */
	public String clicks;

	/** Command responses channel, bidders respond here */
	public String responses;
	
	/** The pixels channel */
	public String pixels;

	/** Xfr port for zerospike */
	public String xfrport;
	
	/** The channel to send commands to bidders on */
	public String commands;
	
	/** The file logger status */
	public String status;
	
	/** The unilogger name */
	public String unilogger;

	public Pubsub pubsub;
	
	/**
	 * Default constructor 
	 */
	public Zeromq() {

	}
}
