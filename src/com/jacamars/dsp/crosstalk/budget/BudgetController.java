package com.jacamars.dsp.crosstalk.budget;

import com.jacamars.dsp.crosstalk.manager.AtomicBigDecimal;
import com.jacamars.dsp.crosstalk.tools.InternalHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Singleton class for controlling budgets using ELK.
 * 
 * @author Ben M. Faul
 *
 */
public enum BudgetController {

	// Instance variable
	INSTANCE;

	/** Banner type */
	public static final String BANNER = "banner";

	/** Video type */
	public static final String VIDEO = "video";

	/** Audio type */
	public static final String AUDIO = "audio";

	/** Native type */
	public static final String NATIVE = "native";

	/** The loop counter */
	public static volatile long revolution;

	/** The hourly aggregator */
	static Aggregator hourly;

	/** The daily aggregator */
	static Aggregator daily;

	/** The total aggregator */
	static Aggregator total;

	/** The last log object */
	static LastLogTracker lastLog;

	/** The prediction interface */
	static Predictions predictor = new Predictions();

	/** This class's logger */
	static final Logger logger = LoggerFactory.getLogger(BudgetController.class);
	
	/** If set, is the debug file that is read to simulate ELK */
	static String debugFile;

	/**
	 * Get an instance of the controller setting the network parameters.
	 * 
	 * @param host1
	 *            String. The aggregator host.
	 * @param host2
	 *            String. The last log object.
	 * @param port
	 *            int. The port the aggegators and the log host uses.
	 * @return BudgetController. The instance.
	 * @throws Exception
	 *             on network errors.
	 */
	public static BudgetController getInstance(String host1, String host2, int port) throws Exception {

		BudgetController.hourly = new Aggregator(Aggregator.HOURLY, host1, port);
		BudgetController.daily = new Aggregator(Aggregator.DAILY, host1, port);
		BudgetController.total = new Aggregator(Aggregator.TOTAL, host2, port);
		BudgetController.lastLog = new LastLogTracker(host1, port);

		ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
		execService.scheduleAtFixedRate(() -> {
			try {
				logger.debug("*** ELK TEST: Getting Aggregate Values {}", revolution);

				BudgetController.process();
			} catch (Exception e) {
				e.printStackTrace();
				// System.exit(0);
			}
		}, 0L, 60000, TimeUnit.MILLISECONDS);

		return INSTANCE;
	}
	
	public static BudgetController getInstance(String simFile) throws Exception {

		debugFile = simFile;
		ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
		execService.scheduleAtFixedRate(() -> {
			try {
				logger.info("*** ELK DEBUG TEST: Getting Aggregate Values {}", revolution);

				BudgetController.debugProcess();
			} catch (Exception e) {
				e.printStackTrace();
				// System.exit(0);
			}
		}, 0L, 60000, TimeUnit.MILLISECONDS);

		return INSTANCE;
	}

	/**
	 * Get the instance.
	 * 
	 * @return BidgetController. The instance.
	 */
	public static BudgetController getInstance() {
		return INSTANCE;
	}
	
	public static void dumpCampaigns(String fileName) throws Exception {
		List<Campaign> campaigns = new ArrayList<Campaign>();
		for (int i=0;i<Aggregator.campaigns.size();i++) {
			Campaign c = Aggregator.campaigns.getNext(i);
			campaigns.add(c);	
		}
		String content = Aggregator.mapper.writer().withDefaultPrettyPrinter().writeValueAsString(campaigns);
		Files.write(Paths.get(fileName), content.getBytes());
	}
	
	public static void debugProcess() throws Exception {
		String content = new String(Files.readAllBytes(Paths.get(debugFile)), StandardCharsets.UTF_8);
		List<Campaign> campaigns = Aggregator.mapper.readValue(content,
				Aggregator.mapper.getTypeFactory().constructCollectionType(List.class, Campaign.class));
		Aggregator.campaigns = new InternalHashMap();
		for (Campaign c : campaigns) {
			Aggregator.campaigns.put(c.id, c);
		}
		revolution++;
		logger.info("Budget Simulation Revolution: {}", revolution);
	}

	/**
	 * Process all the aggregations and log host
	 * 
	 * @throws Exception
	 *             on network errors.
	 */
	static void process() throws Exception {

		Aggregator.touchAll();

		try {
			hourly.query();
			daily.query();
			total.query();
			//if (lastLog != null) // can be null in debug
			//	lastLog.query();

			Aggregator.patchTotals();
			Aggregator.updateGlobal();

			predictor.setScale();
		} catch (Exception error) {
			StringWriter sw = new StringWriter();
			error.printStackTrace(new PrintWriter(sw));
			logger.error("Account watch is postponed because: {}", sw.toString());
		}

		revolution++;

		// dump("285");
		// dumpAllActive();
		logger.info("Latency: {}, revolution: {}", deltaTime(), revolution);
	}

	/**
	 * Print the results
	 */
	public void dump() {
		Aggregator.dump();
	}

	/**
	 * Print the results of a campaign.
	 * 
	 * @param who
	 *            String. The campaign to print.
	 */
	public void dump(String who) {
		Aggregator.dump(who);
	}

	/**
	 * Dump all campaigns whose hourly spend > 0
	 */
	public void dumpAllActive() {
		Aggregator.dumpAllActive();
	}

	/**
	 * Return the revolution number, runs once per minute.
	 * 
	 * @return long. The revolution number - how many times we have processed
	 *         since starting.
	 */
	public long getRevolution() {
		return revolution;
	}

	/**
	 * Return campaign total spend.
	 * 
	 * @param id
	 *            String. The campaign id.
	 * @return double. The campaign's total spend.
	 * @throws Exception
	 *             on Elastic Search errors.
	 */
	public double getCampaignTotalSpend(String id) throws Exception {
		return getCampaignSpend(id, Aggregator.TOTAL);
	}

	/**
	 * Get the campaign spend, based on the mode, TOTAL, DAILY or HOURLY
	 * 
	 * @param id
	 *            String. The campaign id.
	 * @param mode
	 *            int mode. Which item to get, TOTAL, DAILY or HORLY
	 * @return double the campaign spend.
	 * @throws Exception
	 *             on access errors.
	 */
	double getCampaignSpend(String id, int mode) throws Exception {
		Campaign c;
		if (id == null)
			c = Aggregator.global;
		else
			c = Aggregator.get(id);
		if (c == null) {
			c = new Campaign(id);
			Aggregator.add(c);
			return 0;
		}

		double d = get(c, mode);
		return d;
		// return predictor.getEffectiveSpend(d, deltaTime(),
		// c.delta.getAverage());
	}

	/**
	 * Get the daily spend for a campaign.
	 * 
	 * @param id
	 *            String. The campaign if.
	 * @return double. The current daily spend for the campaign./
	 * @throws Exception
	 *             on access errors.
	 */
	public double getCampaignDailySpend(String id) throws Exception {
		return getCampaignSpend(id, Aggregator.DAILY);
	}

	/**
	 * Get the campaign hourly spend..
	 * 
	 * @param id
	 *            String. The id of the campaign.
	 * @return double. The value of the spend.
	 * @throws Exception
	 *             on access errors.
	 */
	public double getCampaignHourlySpend(String id) throws Exception {
		return getCampaignSpend(id, Aggregator.HOURLY);
	}

	/**
	 * Get the creative total spend.
	 * 
	 * @param camp
	 *            String. The campaign id.
	 * @param id
	 *            String. The creative id.
	 * @param type
	 *            String. The type, e.g. 'banner'.
	 * @return double. The total spend.
	 */
	public double getCreativeTotalSpend(String camp, String id, String type) throws Exception {
		return getCreativeSpend(camp, id, type, Aggregator.TOTAL);
	}

	/**
	 * Get the spend, hourly or daily based on type.
	 * 
	 * @param camp
	 *            String. The id of the campaign.
	 * @param id
	 *            String. The id of the creative.
	 * @param type
	 *            String. The type of the creative, e.g. 'banner'
	 * @param mode
	 *            int. The mode, DAILY or HOURLY.
	 * @return double. The spend, hourly or daily depending on type.
	 * @throws Exception
	 *             on access errors.
	 */
	double getCreativeSpend(String camp, String id, String type, int mode) throws Exception {
		Campaign c = Aggregator.get(camp);
		if (c == null)
			return 0;
		Creative cr = c.getCreative(id, type);
		if (cr == null) {
			cr = new Creative(id, type);
			c.addCreative(cr);
			return 0;
		}

		return get(cr, mode);
		// return predictor.getEffectiveSpend(get(cr, mode), deltaTime(),
		// cr.delta.getAverage());
	}

	/**
	 * Get the standard deviation of the minute spend average on a campaign.
	 * 
	 * @param camp
	 *            String. The campaig id.
	 * @return double. The standard deviation.
	 */
	public double getStdDeviationMinuteSpend(String camp) {
		Campaign c;
		if (camp == null)
			c = Aggregator.global;
		else
			c = Aggregator.get(camp);
		if (c == null)
			return Double.NaN;

		return c.delta.getStd();
	}

	/**
	 * Get the standard deviation of the campaign/creative/type average spend.
	 * 
	 * @param camp
	 *            String. The campaign id.
	 * @param creat
	 *            String. The creative id.
	 * @param type
	 *            String the creative type, e.g. 'banner'
	 * @return double. The standard deviation.
	 */
	public double getStdDeviationMinuteSpend(String camp, String creat, String type) {
		Campaign c = Aggregator.get(camp);
		if (c == null)
			return Double.NaN;

		Creative cr = c.getCreative(creat, type);
		if (cr == null)
			return Double.NaN;

		return cr.delta.getStd();
	}

	/**
	 * Get the creative daily spend based on campaign, creative and creative type.
	 * @param camp String. The campaign id.
	 * @param id String. The creative id.
	 * @param type String. The type of the creative, e.g. 'banner'
	 * @return double. The creative's daily spend.
	 * @throws Exception on access errors.
	 */
	public double getCreativeDailySpend(String camp, String id, String type) throws Exception {
		return getCreativeSpend(camp, id, type, Aggregator.DAILY);
	}

	/**
	 * Get the creative hourly spend based on campaign, creative and type.
	 * @param camp String. The campaign id.
	 * @param id String. The creative id.
	 * @param type String. The creative type, e.g., 'banner'
	 * @return double. The creative's hourly spend.
	 * @throws Exception on access errors.
	 */
	public double getCreativeHourlySpend(String camp, String id, String type) throws Exception {
		return getCreativeSpend(camp, id, type, Aggregator.HOURLY);

	}

	/**
	 * Low level get for campaign.
	 * @param c Campaign. The campaign object.
	 * @param mode int. The mode, what to get: TOTAL, DAILY, HOURLY.
	 * @return double. The value in the campaign object based on type.
	 * @throws Exception on access errors.
	 */
	double get(Campaign c, int mode) throws Exception {
		switch (mode) {
		case Aggregator.TOTAL:
			return c.getTotal_price();
		case Aggregator.DAILY:
			return c.getDaily_price();
		case Aggregator.HOURLY:
			return c.getHourly_price();
		}
		throw new Exception("Unknown mode " + mode);
	}

	/**
	 * Low level get for a creative.
	 * @param c Creative. The creative object.
	 * @param mode int. The mode of what to get: TOTAL, DAILY, or HOURLY.
	 * @return double. The returned value based on mode.
	 * @throws Exception on access errors.
	 */
	double get(Creative c, int mode) throws Exception {
		switch (mode) {
		case Aggregator.TOTAL:
			return c.total_price;
		case Aggregator.DAILY:
			return c.daily_price;
		case Aggregator.HOURLY:
			return c.hourly_price;
		}
		throw new Exception("Unknown mode " + mode);
	}

	/**
	 * Close the object's aggregators.
	 * @throws Exception on network errors.
	 */
	public void close() throws Exception {
		BudgetController.hourly.close();
		BudgetController.daily.close();
		BudgetController.total.close();
		BudgetController.lastLog.close();
	}

	/**
	 * Get a campaign's minute spend on average.
	 * @param id String. The campaign id.
	 * @return double. The average minute spend on this campaign.
	 * @throws Exception on JSON parsing errors.
	 */
	public double getCampaignSpendAverage(String id) throws Exception {
		Campaign c;
		if (id == null)
			c = Aggregator.global;
		else
			c = Aggregator.get(id);
		if (c == null)
			throw new Exception("No such campaign " + id);
		return c.delta.getAverage();
	}

	/**
	 * Return the lag time in MINUTES.
	 * 
	 * @return long. The time in minutes the log is behind.
	 */
	public static long deltaTime() {
		if (lastLog == null)    // this is null in debug mode
			return 0;
		
		return lastLog.deltaTime() / 1000;
	}

	public double getCreativeSpendAverage(String camp, String id, String type) throws Exception {
		Campaign c = Aggregator.get(camp);
		if (c == null)
			throw new Exception("No such campaign " + camp);
		Creative cr = c.getCreative(id, type);
		if (cr == null)
			throw new Exception("No such creative " + id);
		return cr.delta.getAverage();
	}

	/**
	 * Determine if the total budget was exceeded. This is different than hourly and daily budget checks, If the total budget
	 * is exceeded the campaign can never restart, and should be removed from the system to save resources.
	 * @param id String. The campaign id to check.
	 * @param total_budget AtomicBigDecimal. This is the total budget, retrieved from MySQL.
	 * @return boolean. Returns true if the total spend is >= total budget.
	 * @throws Exception on errors accessing ELK.
	 */
	public boolean checkCampaignTotalBudgetExceeded(String id, AtomicBigDecimal total_budget) throws Exception {
		if (total != null) {
			double budget = total_budget.getDoubleValue();
			double spend = getCampaignTotalSpend(id);

			logger.debug("TOTAL {} budget: {} vs spend: {}", id, budget, spend);
			if (spend >= budget) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Check a campaign to see if it overspent.
	 * 
	 * @param id
	 *            String. The campaign id.
	 * @param total
	 *            AtomicBigDecimal. The total budget.
	 * @return Boolean. Returns true if overspent or ELK error. Otherwise false.
	 */

	public boolean checkCampaignBudgetsTotal(String id, AtomicBigDecimal total) {
		double spend;
		double budget;
		try {
			if (total != null) {
				budget = total.getDoubleValue();
				spend = getCampaignTotalSpend(id);

				logger.debug("TOTAL {} budget: {} vs spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}
		} catch (Exception error) {
			error.printStackTrace();
			return true;
		}
		return false;
	}

	public boolean checkCampaignBudgetsDaily(String id, AtomicBigDecimal daily) {
		double spend;
		double budget;

		try {
			if (daily != null) {
				budget = daily.getDoubleValue();
				spend = getCampaignHourlySpend(id);
				logger.debug("DAILY {} budget: {} vs spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}
		} catch (Exception error) {
			error.printStackTrace();
			return true;
		}

		return false;
	}

	public boolean checkCampaignBudgetsHourly(String id, AtomicBigDecimal hourly) {
		double spend;
		double budget;

		try {
			if (hourly != null) {
				budget = hourly.getDoubleValue();
				spend = getCampaignDailySpend(id);
				logger.debug("HOURLY {} budget: {} vs spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}
		} catch (Exception error) {
			error.printStackTrace();
			return true;
		}

		return false;
	}

	public boolean checkCampaignBudgets(String id, AtomicBigDecimal total, AtomicBigDecimal daily,
			AtomicBigDecimal hourly) {
		double spend;
		double budget;
		try {
			
			if (total != null) {
				budget = total.getDoubleValue();
				spend = getCampaignTotalSpend(id);

				logger.debug("TOTAL {} budget: {} vs spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}

			if (daily != null) {
				budget = daily.getDoubleValue();
				spend = getCampaignDailySpend(id);			
				logger.debug("DAILY {} budget: {} vs spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}

			if (hourly != null) {
				budget = hourly.getDoubleValue();
				spend = getCampaignHourlySpend(id);
				logger.debug("HOURLY {} budget: {} vs {} spend: {}", id, budget, spend);
				if (spend >= budget) {
					return true;
				}
			}

		} catch (Exception error) {
			error.printStackTrace();
			return true;
		}

		return false;
	}

	/**
	 * Check a campaign/creative to see if it overspent.
	 * 
	 * @param id
	 *            String. The campaign id.
	 * @param crid
	 *            String. The creative id.
	 * @param type
	 *            String. The type of the creative, e.g. 'banner'
	 * @param total
	 *            AtomicBigDecimal. The total budget.
	 * @param daily
	 *            AtomicBigDecimal The daily budget.
	 * @param hourly
	 *            AtonicBigDecimal. The hourly budget.
	 * @return Boolean. Returns true if overspent or ELK error. Otherwise false.
	 */

	public boolean checkCreativeBudgets(String id, String crid, String type, AtomicBigDecimal total,
			AtomicBigDecimal daily, AtomicBigDecimal hourly) {
		double spend;
		double budget;
		try {

			if (total != null && total.doubleValue() != 0) {
				budget = total.getDoubleValue();
				spend = getCreativeTotalSpend(id, crid, type);
				logger.debug("Creative check -------> TOTAL, {}/{} budget: {} vs spend: {}", crid, type, budget, spend);
				if (spend != 0 && spend >= budget)
					return true;
			}

			if (daily != null && daily.doubleValue() != 0) {
				budget = daily.getDoubleValue();
				spend = getCreativeDailySpend(id, crid, type);
				logger.debug("Creative check -------> DAILY {}/{} budget: {} vs spend: {}", id, crid, type, budget,
						spend);
				if (spend >= budget)
					return true;
			}

			if (hourly != null && hourly.doubleValue() != 0) {
				budget = hourly.getDoubleValue();
				spend = getCreativeHourlySpend(id, crid, type);
				logger.debug("Creative check -------> HOURLY {}/{}, budet: {} vs spend: {}", id, crid, type, budget,
						spend);
				if (spend >= budget)
					return true;
			}

		} catch (Exception error) {
			error.printStackTrace();
			return true;
		}

		return false;
	}

	/**
	 * A simple test. 
	 * @param args Stringp[]. Args[0] is the hostname.
	 * @throws Exception on network errors.
	 */

	public static void main(String[] args) throws Exception {
		String host1 = "54.164.51.156";
		String host2 = host1;
		int port = 9200;
		if (args.length > 1) {
			host2 = args[1];
		}
		if (args.length == 3) {
			port = Integer.parseInt(args[2]);
		}
		BudgetController bc = BudgetController.getInstance(host1, host2, port);
		bc.dumpAllActive();
		// bc.dumpCampaigns("debug.sim");
		
		while (bc.getRevolution() < 2) {
			Thread.sleep(1000);
		}
		bc.dumpAllActive();
	}
}
