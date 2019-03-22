package com.jacamars.dsp.crosstalk.budget;

import java.nio.charset.StandardCharsets;



import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;

import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.crosstalk.tools.InternalHashMap;

/**
 * A class that aggregates total, daily and hourly costs for cmapaigns and their creatives.
 * @author Ben M. Faul
 *
 */
public class Aggregator {

	/** Location of the daily file query, Also used for Hourly */
	static final String DAILY_FILE = "query/daily.json";

	/** The total file query */
	static final String TOTAL_FILE = "query/total.json";

	/** Global accumulator */
	static Campaign global = new Campaign("global");

	/** Indicates hourly aggregation */
	public static final int HOURLY = 0;

	/** Indicates daily aggregation */
	public static final int DAILY = 1;

	/** Indicates total cost aggregation */
	public static final int TOTAL = 2;

	/** How many hours past 0 hour */
	public static volatile double hoursPastZero = 0;

	/** How many minutes since last hour */
	public static volatile double minutesPastZero = 0;

	/** Which mode is this aggrgegator */
	final int mode;

	/** Keeps campaigns in a handy map */
	static volatile InternalHashMap<String,Campaign> campaigns = new InternalHashMap();

	/** Holds the contents of the template query */
	final String content;

	/** Tracks network latency on the calls to ELK */
	int latency;

	/** The ELK rest client */
	private RestClient restClient;

	/** A map that holds the returned value from ELK */
	Map map;

	long revolution = 0;

	static final Logger logger = LoggerFactory.getLogger(Aggregator.class);

	/** A handy JSON object for pretty printing */
	public static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	/**
	 * A test program.
	 * @param args String []. The first arg, if supplied, is the ELK host. defaults to 54.164.51.156
	 * @throws Exception on ELK errora.
	 */
	public static void main(String args[]) throws Exception {
		String host = "52.204.44.114";
		if (args.length != 0)
			host = args[0];
		int port = 9200;
		Aggregator hourly = new Aggregator(HOURLY, host, port);
		hourly.query();
		Map m = hourly.getMap();
		String content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(hourly.campaigns);
		Files.write(Paths.get("hourly.sim"), content.getBytes());

		Aggregator daily = new Aggregator(DAILY, host, port);
		daily.query();
		m = daily.getMap();
		content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(daily.campaigns);
		Files.write(Paths.get("daily.sim"), content.getBytes());

		Aggregator total = new Aggregator(TOTAL, host, port);
		total.query();
		total.getMap();
		content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(hourly.campaigns);
		Files.write(Paths.get("total.sim"), content.getBytes());

		int k = 1;
		while (true) {
			hourly.query();
			daily.query();
			total.query();
			logger.debug("Latency = {}, daily latency: {}, hourly latency: {}", total.getLatency(), daily.getLatency() + ", d: " + hourly.getLatency());
			Aggregator.dump();
			Thread.sleep(60000);
			logger.debug("Revolution: {}",k++);
		}
	}

	/**
	 * Create an aggregator.
	 * @param mode int. The mode, TOTAL, DAILY, or HOURLY
	 * @param host String. The ELK host to connect to.
	 * @param port int. The ELK port to connect to.
	 * @throws Exception on network errors.
	 */
	public Aggregator(int mode, String host, int port) throws Exception {
		this.mode = mode;
		if (mode != TOTAL) {
			content = new String(Files.readAllBytes(Paths.get(DAILY_FILE)), StandardCharsets.UTF_8);
		}
		else
			content = new String(Files.readAllBytes(Paths.get(TOTAL_FILE)), StandardCharsets.UTF_8);

		restClient = RestClient.builder(new HttpHost(host, port, "http")).build();
	}

	/**
	 * Close the elk rest client.
	 * @throws Exception on network errors.
	 */
	void close() throws Exception {
		restClient.close();
	}

	/**
	 * Returns the latency of the call inside of ELK.
	 * @return int. The latency in seconds.
	 */
	public int getLatency() {
		return latency;
	}

	/**
	 * Returns the map of the aggregations.
	 * @return Map. The hash map representing the aggregations.
	 */
	public Map getMap() {
		return map;
	}

	/**
	 * Execute the ELK query.
	 * @throws Exception on network errors.
	 */
	public void query() throws Exception {
		/**
		 * Calculate budget offsets
		 */
		Calendar calendar = Calendar.getInstance();
		hoursPastZero = calendar.get(Calendar.HOUR_OF_DAY);
		minutesPastZero = calendar.get(Calendar.MINUTE);
		minutesPastZero += (double)calendar.get(Calendar.SECOND)/60.0;
		hoursPastZero += minutesPastZero/60;

		final Map map;
		String data = content;
		Long now = Instant.now().toEpochMilli();
		String index = "wins-*";

		switch (mode) {
		case DAILY:
			data = data.replaceAll("_NOW_", now.toString());
			Long startDaily = now - (24 * 60 * 60 * 1000);
			data = data.replaceAll("_NOWMINUS_", startDaily.toString());
			data = data.replaceAll("_INTERVAL_", "1d");
			logger.info("Daily aggregation, from: " + new Date(startDaily) + " to: " + new Date(now));
			break;
		case HOURLY:
			data = data.replaceAll("_NOW_", now.toString());
			Long startHourly = now - (60 * 60 * 1000);
			data = data.replaceAll("_NOWMINUS_", startHourly.toString());
			data = data.replaceAll("_INTERVAL_", "1h");
			logger.info("Hourly aggregation, from: " + new Date( startHourly) + " to: " + new Date( now ));
			break;
		default:
			/*
			 * Set thr time to zero hour, minute and millisecond of today.
			 */

	       calendar.set(Calendar.HOUR_OF_DAY, 0);
	       calendar.set(Calendar.MINUTE, 0);
	       calendar.set(Calendar.SECOND, 0);
	       calendar.set(Calendar.MILLISECOND, 0);
				 String nowTime = Long.toString(calendar.getTime().getTime());
				 logger.info("Total aggregation, from: " + new Date(now));

		   data = data.replaceAll("_NOW_", nowTime);
		   index = "bidagg-*";
		}
		
		logger.debug("Query: {}",data);

		HttpEntity entity = new NStringEntity(data, ContentType.APPLICATION_JSON);
		Response indexResponse = null;

		try {
			indexResponse = restClient.performRequest("GET", "/" + index + "/_search",
				Collections.<String, String>emptyMap(), entity);
		} catch (Exception e) {
			// e.printStackTrace();
			logger.error("*** ELK TEST, Index is missing for {}, msg: {}",index, e.getMessage());
			return;
		}

		if (mode == TOTAL)
			doTotal(indexResponse);
		else
			doDailyHourly(indexResponse);

		revolution++;

	}

	/**
	 * Do the total query.
	 * @param indexResponse Response. The returned values from ELK.
	 * @throws Exception on network errors.
	 */
	void doTotal(Response indexResponse) throws Exception {
		String data;
		logger.debug("Index Response: {}", indexResponse);

		data = EntityUtils.toString(indexResponse.getEntity());
		handleTotalData(data);
	}

	/**
	 * The data handler for total data aggrgeations. This is split from doTotal() so that
	 * we can easily use simulated data.
	 * @param data String. The entity data to process.
	 * @throws Exception on JSON errors.
	 */
	void handleTotalData(String data)throws Exception {

		map = mapper.readValue(data, Map.class);
		data = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(map);
		
		logger.debug("Result: {}",data);
		
		latency = (Integer) map.get("took");
		Boolean timedout = (Boolean) map.get("timed_out");
		if (timedout)
			throw new Exception("Timed out");

		Map x = (Map) map.get("aggregations");

		if (x == null) {
			logger.error("*** ELK TEST: Total aggregations are not avialble!");
			return;
		}

		x = (Map) x.get("campaignidagg");
		List<Map> buckets = (List) x.get("buckets");
		double total = 0;
		for (int i = 0; i < buckets.size(); i++) {
			x = (Map) buckets.get(i);
			String campaignid = "" + (Integer) x.get("key");
			Map costagg = (Map) x.get("costagg");
			double price = (Double) costagg.get("value");
			price /= 1000;
			Campaign c = get(campaignid);
			if (c == null) {
				c = new Campaign(campaignid);
				add(c);
			}
			setPrice(c, price);

			x = (Map)x.get("adtypeagg");
			handleTotalCreative(c, x);
			total += price;
		}
		this.map = map;
		// System.out.println("\nTotal: " + total);
	}

	/**
	 * Execute the daily aggregation.
	 * @param indexResponse Response. The returned values from ELK.
	 * @throws Exception on network errors.
	 */
	private void doDailyHourly(Response indexResponse) throws Exception {
		String data;
		logger.debug("IndexResponse: {}", indexResponse);

		data = EntityUtils.toString(indexResponse.getEntity());

		doDailyHourly(data);
	}

	/**
	 * Handle the daily hourly data. This is split from doDailyHourly so we can use simulated days.
	 * @param data String. The data to process.
	 * Note the key "dailyagg" is interpreted to be hourlyagg when the mode is hourly.
	 * @throws Exception on JSON errors.
	 */
	private void doDailyHourly(String data) throws Exception {

		map = mapper.readValue(data, Map.class);
		data = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(map);
		
		logger.debug("Result: {}",data);

		latency = (Integer) map.get("took");
		Boolean timedout = (Boolean) map.get("timed_out");
		if (timedout)
			throw new Exception("Timed out");

		Map x = (Map) map.get("aggregations");
		if (x == null) {                        // no data yet
		    return;
        }
		
		x = (Map) x.get("dailyagg");
		List<Map> buckets = (List) x.get("buckets");

		//if (buckets.size()!=2) {
		//	logger.error("DID NOT GWT 2 BUCKETS ON HOURLY/DAILY {}",data);
		//	return;
		//}

		int pos = buckets.size();
		if (pos == 0) {
			logger.warn("DID NOT GET ANY BUCKETS ON HOURLY/DAILY {}",data);
			return;
		}
		pos--;
		LinkedHashMap campagg = (LinkedHashMap) buckets.get(pos);
		x = (Map) campagg.get("campaignidagg");

		data = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(x);
		// System.out.println(content);

		buckets = (List) x.get("buckets");
		// System.out.println("Buckets: " + buckets.size());

		double total = 0;
		for (int i = 0; i < buckets.size(); i++) {
			x = (Map) buckets.get(i);
			String campaignid = (String) x.get("key");

			// System.out.println(x);
			Map creativeagg = (Map) x.get("adtypeagg");
			x = (Map) x.get("priceagg");
			double price = (Double) x.get("value");
			price /= 1000;

			Campaign c = get(campaignid);
			if (c == null) {
				c = new Campaign(campaignid);
				add(c);
			}
			setPrice(c, price);

			handleCreative(c, creativeagg);
			total += price;
		}
		this.map = map;
	}

	/**
	 * Set the cost in a campaign, based on the mode of this Aggregation.
	 * @param c Campaign. The object holding the campaign.
	 * @param p double. The cost.
	 */
	private void setPrice(Campaign c, double p) {
		switch (mode) {
		case DAILY:
			c.setDaily_price(p);
			logger.debug("SET DAILY PRICE: {} in Campaign {}", p, c.id);
			break;
		case HOURLY:
			c.delta.add(p);
			c.setHourly_price(p);
			
			logger.debug("SET HOURLY PRICE: {} in Campaign {}",p,c.id);
			
			break;
		default:
			c.setTotal_price(p);
			break;
		}
	}

	/**
	 * Set the cost of the Creative, based on the mode.
	 * @param c Creative. The object holding the creative.
	 * @param p double. The cost.
	 */
	private void setPrice(Creative c, double p) {
		switch (mode) {
		case DAILY:
			c.daily_price = p;
			break;
		case HOURLY:
			c.delta.add(p);
			c.hourly_price = p;
			break;
		default:
			c.total_price = p;
			break;
		}
	}

	/**
	 * Parses the results of aggregation and puts it into the campaign.
	 * @param c Campaign. The object representing the campaign.
	 * @param cr Map. The aggregation results.
	 */
	private void handleTotalCreative(Campaign c, Map cr) {
		List<Map> list = (List) cr.get("buckets");
		// System.out.println("\tBuckets: " + list.size());
		for (Map x : list) {
			String key = (String) x.get("key");
			Map cx = (Map) x.get("creativeidagg");
			List<Map> buckets = (List) cx.get("buckets");
			double total = 0;
			for (Map agg : buckets) {
				String id = "" + (Integer) agg.get("key");
				Map pagg = (Map) agg.get("costagg");
				double price = (Double) pagg.get("value");
				price = price /= 1000;

				Creative creat = c.getCreative(id,key);
				if (creat == null) {
					creat = new Creative(key, id);
					c.addCreative(creat);
				}
				setPrice(creat, price);
			}
		}
	}

	/**
	 * Handle the creatives in a campaign, from the results of the aggregation.
	 * @param c Campaign. The object representing the campaign.
	 * @param cr Map. The aggregations of all the creatives in the campaign.
	 */
	private void handleCreative(Campaign c, Map cr) {
		List<Map> list = (List) cr.get("buckets");

		for (Map x : list) {
			String key = (String) x.get("key");
			Map cx = (Map) x.get("creativeagg");
			List<Map> buckets = (List) cx.get("buckets");
			double total = 0;
			for (Map agg : buckets) {
				String id = (String) agg.get("key");
				Map pagg = (Map) agg.get("priceagg");
				double price = (Double) pagg.get("value");
				price = price /= 1000;

				Creative creat = c.getCreative(id, key);
				if (creat == null) {
					creat = new Creative(key, id);
					c.addCreative(creat);
				}
				setPrice(creat, price);
			}

		}
	}

	////////////////////////////

	/**
	 * Add a campaign to the static map.
	 * @param c Campaign. The campaign to put into the map.
	 */
	public static void add(Campaign c) {
		campaigns.put(c.id,c);
	}

	/**
	 * Clear the campaigns map.
	 */
	public static void clear() {
		campaigns.clear();
	}

	/**
	 * Return a list of campaign ids.
	 * @return List. The list of campaign id strings.
	 */
	public static List<String> getCampaigns() {
		List<String> ids = new ArrayList<String>();
		int i = 0;
		String x = null;
		while((x=campaigns.getKey(i++)) != null) {
			ids.add(x);
		}
		return ids;
	}

	/**
	 * Return a campaign given it's key.
	 * @param id String. The id of the campaign.
	 * @return Campaign. The campaign keyed by id.
	 */
	public static Campaign get(String id) {
		return (Campaign)campaigns.get(id);
	}

	/**
	 * Return a list of creative ids with the campaign.
	 * @param id String. The campaign id key.
	 * @return List. A list of strings of the creatives for this campoaign.
	 */
	public static List<Creative> getCreatives(String id) {
		Campaign c = get(id);
		return c.creatives;
	}

	/**
	 * Return whether the specified campaign/creative/type exists.
	 * @param campaign String. The campaign id to search for.
	 * @param creative String. The creative id to search for.
	 * @param type String. The type of the creative.
	 * @return boolean. Returns true if found. else returns false.
	 */
	public static boolean creativeExists(String campaign, String creative, String type) {
		Campaign c = get(campaign);
		if (c == null)
			return false;

		Creative cr = c.getCreative(campaign, type);
		if (cr == null)
			return false;
		return true;
	}

	/**
	 * Patch the totals in the aggregations with the appropriate daily_price. Use this when
	 * total aggregations is at 0 hour instead of the default, which is 5 minutes old.
	 */
	public static void patchTotals() {
		int i = 0;
		Campaign entry = null;
		while((entry=campaigns.getNext(i++)) != null) {
			entry.patchTotals();
		}
	}

	/**
	 * Update the global counter. Make sure you call this after pa
	 */
	public static void updateGlobal() {
		global.setTotal_price(0);
		global.setDaily_price(0);
		global.setHourly_price(0);

		int i = 0;
		Object x = null;
		while((x=campaigns.getNext(i++)) != null) {
			Campaign entry = (Campaign)x;
			double d = global.getTotal_price();
			global.setTotal_price(d + entry.getTotal_price());

			d = global.getDaily_price();
			global.setDaily_price(d + entry.getDaily_price());

			d = global.getHourly_price();
			global.setHourly_price(d + entry.getHourly_price());
		}



		global.delta.add(global.getHourly_price());
	}

	/**
	 * Pretty print dump all the campaigns and creatives.
	 */
	public static void dump() {
		logger.info("********************************************");

		for (int i=0;i<campaigns.size();i++) {
			Campaign c = campaigns.getNext(i++);

			//if (c.delta != 0) {
				logger.info("{}, total: {}, daily: {}. hourly: {}, delta: {}",c.id,
						c.getTotal_price(),c.getDaily_price(),c.getHourly_price(),c.delta.getAverage());

				double hourly = c.delta.getAverage() * 60;
				double daily = c.delta.getAverage() * 3600;
				logger.info("Predict: Daily: {}, Hourly: {}",daily, hourly);

				for (Creative cc : c.creatives) {
			//		if (cc.delta != 0) {
						logger.info("{}/{}/{}, Total: {}, Daily: {}, Hourly: {}, Delta: {}", c.id,cc.id,cc.adtype,cc.total_price,
							cc.daily_price,cc.hourly_price,cc.delta.getAverage());
				//	}
				}

			//}
		}
		logger.info("********************************************");
	}

	/**
	 * Pretty print dump all the campaigns and creatives.
	 */
	 static void dumpAllActive() {
		logger.info("********************************************");
		for (int i=0;i<campaigns.size();i++) {
			Campaign c = campaigns.getNext(i++);
			if (c.getHourly_price() != 0) {
				logger.info("{}, total: {}, daily: {}. hourly: {}, delta: {}",c.id,
						c.getTotal_price(),c.getDaily_price(),c.getHourly_price(),c.delta.getAverage());

				double hourly = c.delta.getAverage() * 60;
				double daily = c.delta.getAverage() * 3600;
				logger.info("Predict: Daily: {}, Hourly: {}",daily, hourly);

				for (Creative cc : c.creatives) {
					if (cc.hourly_price != 0) {
						logger.info("{}/{}/{}, Total: {}, Daily: {}, Hourly: {}, Delta: {}", c.id,cc.id,cc.adtype,cc.total_price,
							cc.daily_price,cc.hourly_price,cc.delta.getAverage());
					}
				}

			}
		}
		logger.info("********************************************");
	}

	/**
	 * Dump the indicated campaign to stdout.
	 * @param who String. The campaign to dump.
	 */
	 static void dump(String who) {
		logger.info("********************************************");
		for (int i=0;i<campaigns.size();i++) {
			Campaign c = campaigns.getNext(i++);
			if (c.id.equals(who)) {
				logger.info("{}, Total: {}, Hourly: {}, Daily: {}, delta: {}",
						c.id,c.getTotal_price(),c.getDaily_price(),c.getHourly_price(),c.delta.getAverage());

				double hourly = c.delta .getAverage() * 60;
				double daily = c.delta.getAverage() * 3600;
				logger.info("Predict, Daily: {}, Hourly: {}",daily, hourly);

				for (Creative cc : c.creatives) {
					//if (cc.id.equals("420")) {
						logger.info("{}/{}/{}, Total: {}, Daily: {}, Hourly: {}, Delta: {}",who,cc.id,cc.adtype,cc.total_price,
								cc.daily_price,cc.hourly_price,c.delta.getAverage());
					//}
				}

			}
		}
		logger.info("********************************************");
	}

	static void touchAll() {
		for (int i=0;i<campaigns.size();i++) {
			Campaign c = campaigns.getNext(i);
			c.touch();
		}
	}
}

/**
 * A builtin class for maintaining aggregations for campaigns and creatives. Used only within the budgeting part
 * of the system. No fancy synchronization needed, only 1 thread writes, all others read.
 * @author Ben M. Faul
 *
 */
class Campaign {

	/** The creatives of this campaign */
	public List<Creative> creatives = new ArrayList<Creative>();

	/** Campaign id */
	public String id;

	/** The hourly cost incurred since the last hour mark */
	private volatile double hourly_price;
	private boolean setHourly = false;

	/** The daily cost incurred since the last day mark */
	private volatile double daily_price;
	private boolean setDaily = false;

	/** The total price since campaign creation */
	private volatile double total_price;
	private boolean setTotal = false;

	/** price incurred per minute */
	MovingAverage delta = new MovingAverage(10);

	/**
	 * Constructor for the campaign aggregation tracker.
	 * @param id String. The campaign id.
	 */
	public Campaign(String id) {
		this.id = id;
	}

	/**
	 * Constructor for JSON
	 */
	public Campaign() {

	}

	/**
	 * Given the creative id and its type, return creative object.
	 * @param id String. The id of the creative.
	 * @param type String. The type eg 'banner'
	 * @return Creative. The object if found, or, null.
	 */
	public Creative getCreative(String id, String type) {
		for (Creative c : creatives) {
			if (c.adtype.equals(type) && c.id.equals(id))
				return c;
		}
		return null;
	}

	/**
	 * Patch the total_price by adding in the daily_price. Use this when total aggregations are at the
	 * 0 hour mark of the day. Using the standard aggregation causes a delay of 5 minutes otherwise.
	 * Note, this also will reset hourly and daily cost to 0 if there were no aggregations
	 */
	public void patchTotals() {
		if (!setTotal) {
			setTotal = true;
			total_price = 0;
		}

		total_price += daily_price;
		for (int i=0;i<creatives.size();i++) {
			Creative c = creatives.get(i);
			c.patchTotals();
		}

		if (!setDaily) {
			setDaily = true;
			daily_price = 0;
		}

		if (!setHourly) {
			setHourly = true;
			hourly_price = 0;
		}
	}

	/**
	 * Touch the campaign hourly and daily totals. If the budget is exceeded for one of these, then the aggrehgator will not
	 * report any wins (cuz the budget is exceeded), so the data will not appear and these values will be left over. They
	 * will never roll over to 0 and then will not bid when the bidget is now available. Note no need to do the total. because
	 * there will always be a total aggregation of the campaign ever won anything at all.
	 *
	 * Call this before you process the aggrheations.
	 */
	public void touch() {
		setHourly = false;
		setDaily = false;
		setTotal = false;
	}

	/**
	 * Return the hourly price.
	 * @return double. The current running total.
	 */
	public double getHourly_price() {
		return hourly_price;
	}

	/**
	 * Return the daily price.
	 * @return double. The current running total.
	 */
	public double getDaily_price() {
		return daily_price;
	}

	/**
	 * Return the totaal price.
	 * @return double. The current running total.
	 */
	public double getTotal_price() {
		return total_price;
	}

	/**
	 * Set the hourly price.
	 * @param d double. The value to set hourly to.
	 */
	public void setHourly_price(double d) {
		hourly_price = d;
		setHourly = true;
	}

	/**
	 * Set the daily price.
	 * @param d double. The value to set daily to.
	 */
	public void setDaily_price(double d) {
		daily_price = d;
		setDaily = true;
	}

	/**
	 * Set the total price.
	 * @param d double. The price to set totsal to.
	 */
	public void setTotal_price(double d) {
		total_price = d;
		setTotal = true;
	}

	/**
	 * Add a creative to the campaign.
	 * @param c Creative. The creative to add.
	 */
	public void addCreative(Creative c) {
		creatives.add(c);
	}

}

/**
 * The internal class to track aggregations at the creative level. No need for fancy
 * syncrhonization, only 1 thread updates, all other threads simply read it.
 * @author Ben M. Faul
 *
 */
class Creative {
	/** Ad type as in 'banner' or 'video' */
	volatile public String adtype;

	/** The creative id */
	public String id;

	/** The hourly cost since the last hour */
	volatile public double hourly_price;

	/** The daily cost, since midnight */
	volatile public double daily_price;

	/** The total price, if not patched, is total cost as of 5 mins ago */
	volatile public double total_price;

	/** Rate of spend over last minute */
	public MovingAverage delta = new MovingAverage(60);

	/**
	 * Creates the creative.
	 * @param adtype String. The type, as in 'banner' or 'video'
	 * @param id String. The creative id.
	 */
	public Creative(String adtype, String id) {
		this.adtype = adtype;
		this.id = id;
	}

	/**
	 * Constructor for JSON to use.
	 */
	public Creative() {

	}

	/**
	 * Patch the total by adding in the daily_price. Use this when aggregations of the total
	 * are at zero hour instead of the default (which is 5 minutes old
	 */
	public void patchTotals() {
		total_price += daily_price;
	}
}
