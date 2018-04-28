package com.jacamars.dsp.crosstalk.config;

/**
 * Top level configuration object
 * @author Ben M. Faul
 *
 */
public class Config {

	/** Region */
	public String region;
	
	/** The 'user' name */
	public String rtbName;
	
	/** The password */
	public String password;
	
	/** The App application object */
	public App app;
	
	/* The mysql configuration object */
	public Mysql mysql;
	
	/** The aerospike configuration object */
	public Aerospike aerospike;
	
	/** The 0MQ configuration object */
	public Zeromq zeromq;
	
	/** The Elastic Search configuration object */
	public Elk elk;
	
	/**
	 * Default constructor
	 */
	public Config() {
		
	}

	public void fixRegion() {
		if (region.startsWith("$")) {
			String name = region.substring(1);
			region = System.getenv(name);
		}
	}
}
