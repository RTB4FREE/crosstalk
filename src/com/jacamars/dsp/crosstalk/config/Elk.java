package com.jacamars.dsp.crosstalk.config;

/**
 * Elastic Search configuration object
 * @author Ben M. Faul
 *
 */
public class Elk {

	/** The host of where to get daily and hourly spends */
	public String host;

	/** The host of where to get the total spends */
	public String agghost;

	/** The port to use for web client access to ELK */
	public String port;

	/** If specified, for debugging where to read the hourly campaign data from */
	public String simFile;

	/**
	 * Default constructor
	 */
	public Elk() {

	}

	public int getPort() {
		if (port == null){
			return 9200; // default port
		}

		if (port.startsWith("$")) {
			return Integer.parseInt(System.getenv(port.substring(1)));
		}
		else return Integer.parseInt(port);
	}

	public String getHost() {
		if (host.startsWith("$")) {
			String name = host.substring(1);
			host = System.getenv(name);
		}
		return host;
	}

	public String getAggHost() {
		if (agghost.startsWith("$")) {
			String name = agghost.substring(1);
			agghost = System.getenv(name);
		}
		return agghost;
	}
}
