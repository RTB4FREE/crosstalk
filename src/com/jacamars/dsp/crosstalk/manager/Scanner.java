package com.jacamars.dsp.crosstalk.manager;

import java.net.InetAddress;



import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jacamars.dsp.rtb.redisson.RedissonClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.budget.BudgetController;
import com.jacamars.dsp.crosstalk.tools.ResultSetToJSON;
import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.commands.DeleteCampaign;

import com.jacamars.dsp.rtb.commands.StartBidder;
import com.jacamars.dsp.rtb.commands.StopBidder;
import com.jacamars.dsp.rtb.common.Campaign;

import com.jacamars.dsp.rtb.tools.Performance;

/**
 * The periodic processing for campaigns and creatives.
 * 
 * @author Ben M. Faul
 *
 */
public class Scanner implements Runnable, ScannerIF {

	/** Seconds before the deadman switch will fire */
	public static final int DEADMAN_TIMEOUT = 30;

	public static long runCount = 0;
	static Configuration config;
	Thread me;
	Date now = new Date();
	Timestamp nowt = new Timestamp(now.getTime());
	ResultSet rs = null;
	Timestamp udt = null;
	private ObjectMapper mapper = new ObjectMapper();

	static public Scanner instance;
	RedissonClient redis;

	public static boolean forceRun = true;

	public static Map<Integer, JsonNode> gloablRtbSpecification;
	public static ArrayNode campaignRtbStd;
	public static ArrayNode bannerRtbStd;
	public static ArrayNode videoRtbStd;
	public static ArrayNode exchangeAttributes;

	boolean isRunning = false;

	/**
	 * Campaigns in a list, for comparing against periodic selects of the
	 * campaigns db
	 */
	public static Set<AccountingCampaign> campaigns = new ConcurrentHashMap<Object, Object>().newKeySet();

	public static Map<String, AccountingCampaign> deletedCampaigns = new ConcurrentHashMap<String, AccountingCampaign>();
	/**
	 * creatives in a map, for fast lookup on win notifications and rollups.
	 * Note creatives are not deleted
	 */
	public static volatile Map<String, AccountingCreative> creativesMap = new ConcurrentHashMap<String, AccountingCreative>();

	/** Set of camaigns that need sweeping */
	static final Set<String> sweepSet = new HashSet<String>();

	/**
	 * The list of RTB rules not specified in campaigns, creatives and targets.
	 */
	public static final String RTB_STD = "rtb_standards";
	public static final String CAMP_RTB_STD = "campaigns_rtb_standards";
	public static final String BANNER_RTB_STD = "banners_rtb_standards";
	public static final String VIDEO_RTB_STD = "banner_videos_rtb_standards";

	ScheduledExecutorService execService;;

	// The singleton that interfaces with ELK
	public static BudgetController budgets;
	
	// sl4j logger
	protected static final Logger logger = LoggerFactory.getLogger(Scanner.class);

	// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public ArrayNode createJson() throws Exception {
		Date now = new Date();
		Timestamp update = new Timestamp(now.getTime());

		Configuration config = Configuration.getInstance();
		//String select = "select * from campaigns where  status = 'runnable' and activate_time <= ? and expire_time > ?";
		String select = "select * from campaigns where status='runnable'";

		PreparedStatement prep;

		try {
			prep = config.getConnection().prepareStatement(select);
		} catch (Exception error) {
			retrySql();
			prep = config.getConnection().prepareStatement(select);
		}

		ResultSet rs = prep.executeQuery();
		ArrayNode nodes = ResultSetToJSON.convert(rs);
		handleNodes(nodes);

		return nodes;
	}

	public ArrayNode createJson(String id) throws Exception {
		Date now = new Date();
		Timestamp update = new Timestamp(now.getTime());

		Configuration config = Configuration.getInstance();

		String select = "select * from campaigns where id = " + id;

		PreparedStatement prep;

		try {
			prep = config.getConnection().prepareStatement(select);
		} catch (Exception error) {
			retrySql();
			prep = config.getConnection().prepareStatement(select);
		}

		ResultSet rs = prep.executeQuery();

		ArrayNode nodes = ResultSetToJSON.convert(rs);
		handleNodes(nodes);

		return nodes;
	}

	/**
	 * Convert SQL tables for campaigns, creatives, target and rtb_standard into JSON object
	 * @param nodes JSON array to hold the results.
	 * @throws Exception on JSON or SQL errors.
	 */
	public static void handleNodes(ArrayNode nodes) throws Exception {

		ResultSet rs;
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		List<Integer> list = new ArrayList<Integer>();

		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode) nodes.get(i);
			int campaignid = x.get("id").asInt();
			String regions = x.get("regions").asText();
			regions = regions.toLowerCase();
			if (regions.contains(Configuration.getInstance().config.region.toLowerCase())) {
				int targetid = x.get("target_id").asInt();
				rs = stmt.executeQuery("select * from targets where id = " + targetid);
				ArrayNode inner = ResultSetToJSON.convert(rs);
				ObjectNode y = (ObjectNode) inner.get(0);
				x.set("targetting", y);
			} else {
				list.add(i);
			}
		}

		//
		// Remove in reverse all that don't belong to my region/
		//
		while (list.size() > 0) {
			Integer x = list.get(list.size() - 1);
			nodes.remove(x);
			list.remove(x);
		}

		if (nodes.size() == 0)
			return;

		// /////////////////////////// GLOBAL rtb_spec
		gloablRtbSpecification = new HashMap<Integer, JsonNode>();
		rs = stmt.executeQuery("select * from " + RTB_STD);
		ArrayNode std = ResultSetToJSON.convert(rs);
		Iterator<JsonNode> it = std.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			gloablRtbSpecification.put(child.get("id").asInt(), child);
		}

		campaignRtbStd = ResultSetToJSON.factory.arrayNode();
		rs = stmt.executeQuery("select * from " + CAMP_RTB_STD);
		std = ResultSetToJSON.convert(rs);
		it = std.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			campaignRtbStd.add(child);
		}

		bannerRtbStd = ResultSetToJSON.factory.arrayNode();
		rs = stmt.executeQuery("select * from " + BANNER_RTB_STD);
		std = ResultSetToJSON.convert(rs);
		it = std.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			bannerRtbStd.add(child);
		}

		videoRtbStd = ResultSetToJSON.factory.arrayNode();
		rs = stmt.executeQuery("select * from " + VIDEO_RTB_STD);
		std = ResultSetToJSON.convert(rs);
		it = std.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			videoRtbStd.add(child);
		}

		exchangeAttributes = ResultSetToJSON.factory.arrayNode();
		rs = stmt.executeQuery("select * from exchange_attributes");
		std = ResultSetToJSON.convert(rs);
		it = std.iterator();
		while (it.hasNext()) {
			JsonNode child = it.next();
			exchangeAttributes.add(child);
		}
		// ////////////////////////////////////////////////////////////////////////////
		// Banner
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode) nodes.get(i);
			int campaignid = x.get("id").asInt();
			rs = stmt.executeQuery("select * from banners where campaign_id = " + campaignid);
			ArrayNode inner = ResultSetToJSON.convert(rs);
			x.set("banner", inner);
		}

		// Video
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode) nodes.get(i);
			int campaignid = x.get("id").asInt(); ///////// CHECK
			rs = stmt.executeQuery("select * from banner_videos where campaign_id = " + campaignid);
			ArrayNode inner = ResultSetToJSON.convert(rs);
			x.set("banner_video", inner);
		}
		
		// Audio
		for (int i = 0; i < nodes.size(); i++) {
			ObjectNode x = (ObjectNode) nodes.get(i);
			int campaignid = x.get("id").asInt(); 
			try {
				rs = stmt.executeQuery("select * from banner_audio where campaign_id = " + campaignid);
				ArrayNode inner = ResultSetToJSON.convert(rs);
				x.set("banner_audio", inner);
			} catch (Exception error) {
				if (!error.getMessage().contains("doesn't exist"))
					throw error;
					
			}
		}
	}


	public AccountingCampaign makeNewCampaign(ObjectNode node) throws Exception {
		return new AccountingCampaign(node);
	}

	public String getId(ObjectNode node) {
		return "" + node.get("id").asInt();
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	public Scanner() {
		instance = this;
	}

	public Scanner(Configuration config) {
		this.config = config;
		instance = this;
	}

	public static Scanner getInstance() {
		return instance;
	}

	public static void startCommand() throws Exception {

		StartBidder startcmd = new StartBidder("");
		startcmd.from = config.getApp().uuid;
		startcmd.id = UUID.randomUUID().toString();

		config.sendCommand(true,startcmd);
	}

	public void start() {

		try {
			if (!(isRunning = initialize()))
				return;

			startCommand();

		} catch (Exception e) {
			e.printStackTrace();
			isRunning = false;
			return;
		}

		logger.info("Initialized {} campaigns",campaigns.size());

		me = new Thread(this);

		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		me.start();
		isRunning = true;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public boolean initialize() throws Exception {
		InetAddress localMachine = java.net.InetAddress.getLocalHost();
		String useName = localMachine.getHostName();

        ScheduledExecutorService deadman = Executors.newScheduledThreadPool(1);
        deadman.scheduleAtFixedRate(() -> {
            try {
                Configuration c = Configuration.getInstance();
                c.deadmanSwitch.updateKey(c.deadmanKey);
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }, 0L, 30000, TimeUnit.MILLISECONDS);



        campaigns.clear();
		creativesMap.clear();
		sweepSet.clear();

		logger.info("Campaign Manager start");

		//
		// Clear the Aerospike Database
		//
		try {
			config.shared.clear();
		} catch (Exception e1) {
			logger.error("FATAL ERROR, can't clear the Aerospike database");
			e1.printStackTrace();
			return false;
		}

		//
		// Tell the bidders to clear their memories.
		//
		// DeleteCampaign del = new DeleteCampaign("",
		// Configuration.getInstance().config.rtbName, "*");
		// del.from = config.getApp().uuid;
		// del.id = UUID.randomUUID().toString();
		// config.sendCommand(del);

		Calendar calendar = Calendar.getInstance();

		return true;
	}

	public void retrySql() {
		panicStopBidders("MySQL Error, Crosstalk can't connect to SQL");
		try {
			if (Configuration.getInstance().getConnection().isClosed()) {
				int i = 0;
				while (Configuration.getInstance().getConnection().isClosed() && i < 30) {
					Configuration.getInstance().reconnectMySql();
					if (Configuration.getInstance().getConnection().isClosed()) {
						logger.error("SQL Connection attempt {} failed, retry in 5 seconds", i);
						Thread.sleep(10000);
					}
					i++;
				}
			}
			if (Configuration.getInstance().getConnection().isClosed()) {
				logger.error("SQL Connection attempts all failed, TERMINATING");
				panicStopBidders("MySQL Error, Crosstalk can't connect");
				System.exit(0);
			}
		} catch (Exception error) {
			error.printStackTrace();
			panicStopBidders("MySQL Error, Crosstalk can't connect");
			System.exit(0);
		}
		logger.info("SQL Connection re-established.");
		Crosstalk.needMasterUpdate = true;
	}

	AccountingCampaign getKnownCampaign(String id) {
		AccountingCampaign camp = deletedCampaigns.get(id);
		if (camp != null)
			return camp;

		for (AccountingCampaign c : campaigns) {
			if (c.campaign.adId.equals(id))
				return c;
		}
		return null;
	}

	/**
	 * Return the node for a single campaign
	 * @param id String. The campaign id
	 * @return ObjectNode. The JSON representatio of the SQL query
	 * @throws Exception on SQL errors
	 */
	ObjectNode getNode(String id) throws Exception {
		ArrayNode array = createJson(id); // get a copy of the SQL database
		for (int i = 0; i < array.size(); i++) {
			ObjectNode node = (ObjectNode) array.get(i);
			String sid = getId(node);
			if (sid.equals(id))
				return node;
		}
		return null;
	}

	public List<String> delete(String campaign) throws Exception {
		AccountingCampaign c = getKnownCampaign(campaign);
		c.setStatus("offline");
		campaigns.remove(c);
		parkCampaign(c);
		c.removeFromRTB();
		return null;
	}

	/**
	 * Add a single campaign back into the system (usually from the API.
	 * 
	 * @param campaign
	 *            String. The campaign id.
	 * @return String. The response to send back.
	 * @throws Exception
	 *             on SQL errors.
	 */
	public String add(String campaign) throws Exception {
		ArrayNode array = createJson(campaign);
		if (array == null || array.size() == 0) {
			throw new Exception("Campaign is unknown: " + campaign);
		}
		JsonNode node = (JsonNode) array.get(0);
		return update(node,true);
	}

	public String update(JsonNode jnode, boolean add) throws Exception {
		ObjectNode node = (ObjectNode) jnode;
		if (jnode.get("id") == null)
			throw new Exception("Campaign did not exist");

		String campaign = jnode.get("id").asText();
		List<String> bidders = new ArrayList<String>();
		String msg = null;

		AccountingCampaign c = getKnownCampaign(campaign);

		if (c == null && node == null) {
			throw new Exception("Campaign is unknown: " + campaign);
		}

		// New campaign
		if (c == null && node != null) {
			c = makeNewCampaign(node);
			if (c.isActive()) {
				c.runUsingElk();
				logger.info("New campaign {} going active",campaign);
				msg = "NEW CAMPAIGN GOING ACTIVE: " + campaign;
				campaigns.add(c);
				try {
					if (add)
						c.addToRTB(); // notifies the bidder
				} catch (Exception error) {
					logger.error("Error: Failed to load {} into bidder, reason: {}", c.campaignid, error.toString());
				}
			} else {
				logger.info("New campaign is inactive {}, reason: {}", campaign, c.report());
				//campaigns.remove(c);
				//deletedCampaigns.put(campaign, c);
				delete(campaign);
			}
			// handleLucene(c);
			// Deleted campaign
		} else if (node == null && c != null) {
			logger.info("Deleting a campaign: {}",campaign);
			msg = "DELETED CAMPAIGN: " + campaign;
			c.report();
			c.stop();
			campaigns.remove(c);
			deletedCampaigns.put(campaign, c);
			parkCampaign(c);

			// Known campaign
		} else {
			// System.out.println(node);
			c.update(node);
			if (c.isActive()) {
				logger.info("Previously inactive campaign going active: {}",campaign);
				if (deletedCampaigns.get(campaign) != null) {
					deletedCampaigns.remove(campaign);
					// campaigns.add(c);
				}
				msg = "CAMPAIGN GOING ACTIVE: " + campaign;
				try {
					c.addToRTB(); // notifies the bidder
				} catch (Exception err) {
					logger.error("Failed to load campaign {} into bidders, reason: {}", c.campaignid,err.toString());
				}
			} else {
				if (c.canBePurged()) {
					logger.info("New campaign is purgeable:{}, reason: {}", campaign,c.report());
				} else {
					logger.info("New campaign going inactive:{}, reason: {}", campaign, c.report());
					msg = "CAMPAIGN GOING INACTIVE: " + campaign + ", reason: " + c.report();
					campaigns.remove(c);
					deletedCampaigns.put(campaign, c);
					parkCampaign(c); // notifies the bidder
				}
			}
			// handleLucene(c);
		}

		return msg;
	}

	public void updateArray(ArrayNode jnodes) throws Exception {

		for (JsonNode jnode : jnodes) {
			ObjectNode node = (ObjectNode) jnode;
			String campaign = jnode.get("id").asText();
			logger.info("Scanner:updateArray", "Sending update command: {}", campaign);

			AccountingCampaign c = getKnownCampaign(campaign);

			if (c == null && node == null) {
				throw new Exception("Campaign is unknown: " + campaign);
			}

			// New campaign
			if (c == null && node != null) {
				c = makeNewCampaign(node);
				if (c.isActive()) {
					logger.info("New campaign going active:{}", campaign);
					campaigns.add(c);
				} else {
					logger.info("New campaign is inactive: {}, reason: {}",campaign , c.report());
					campaigns.remove(c);
					deletedCampaigns.put(campaign, c);
				}
				// handleLucene(c);
				// Deleted campaign
			} else if (node == null && c != null) {
				logger.info("Deleting a campaign: " + campaign);
				c.report();
				campaigns.remove(c);
				deletedCampaigns.put(campaign, c);
				parkCampaign(c);

				// Known campaign
			} else {
				// System.out.println(node);
				c.update(node);
				if (c.isActive()) {
					logger.info("Previously inactive campaign going active: {}", campaign);
					if (deletedCampaigns.get(campaign) != null) {
						deletedCampaigns.remove(campaign);
						campaigns.add(c);
					}
				} else {
					logger.info("Campaign going inactive: " + campaign + ", reason: " + c.report());
					campaigns.remove(c);
					deletedCampaigns.put(campaign, c);
					parkCampaign(c); // notifies the bidder
				}
				// handleLucene(c);
			}
		}

		List<String> clist = new ArrayList<String>();
		for (AccountingCampaign camp : campaigns) {
			if (camp.isActive())                         // make sure you don't load any inactive campaign
				clist.add("" + camp.campaignid);
		}
		if (clist.size() > 0)
			Configuration.getInstance().sendUpdateList(clist);
	}

	/**
	 * Update a command, note retrieves the campaign from the SQL database,
	 * 
	 * @param campaign
	 *            String.
	 * @return String. The message on return.
	 * @throws Exception
	 *             on SQL errors.
	 */
	public String update(String campaign) throws Exception {
		AccountingCampaign c = getKnownCampaign(campaign);
		ObjectNode node = getNode(campaign);

		if (node == null) {
			throw new Exception("No such campaign: " + campaign);
		}
		return update(node,true);
	}

	/**
	 * Updates a bidder with the loaded campaigns.
	 * 
	 * @param bidder
	 *            String. The bidder to loas.
	 * @return List. The list of loaded campaigns.
	 * @throws Exception
	 *             on 0MQ errors.
	 */
	public List<String> refreshBidder(String bidder) throws Exception {
		logger.info("New bidder being processed: {}", bidder);
		return Configuration.getInstance().sendCampaignList(bidder, campaigns);
	}

	/**
	 * Refresh the system. Load all bidders with all runnable campaigns.
	 * 
	 * @return List. The bidder list.
	 * @throws Exception
	 *             on SQL or 0MQ errors.
	 */
	public List<String> refresh() throws Exception {
		logger.info("Sending global refresh");

		DeleteCampaign del = new DeleteCampaign("*","*");
		config.sendCommand(false,del);

		Thread.sleep(1000);

		ArrayNode array = createJson(); // get a copy of the SQL database

		logger.info("Campaigns:scanner:refresh", "********** SENDING UPDATES **********");
		long time = System.currentTimeMillis();
	
		
		ExecutorService executor = Executors.newFixedThreadPool(16);

		for (JsonNode s : array) {
			Runnable w = new CampaignBuilderWorker(s);
			executor.execute(w);
			//update(s,false);
		}

		time = System.currentTimeMillis() - time;
		time /= 1000;
		logger.info("Updates took {} seconds",time);
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		
		for (AccountingCampaign c : campaigns) {
			c.updateSpike();
		}
		
		return Configuration.getInstance().sendCampaignList("", campaigns);
		//return cInDB; */
	}


	public void run() {

	    try {
			refresh();
			Thread.sleep(60000);
		} catch (Exception e) {
			e.printStackTrace();
			if (e.toString().toLowerCase().contains("mysql")) {
				System.err.println("SQL Error, Goodbye");
				System.exit(0);
			}
			return;
		}

		while (true) {
			try {
				for (AccountingCampaign camp : campaigns) {
					camp.runUsingElk();
								
					if (!camp.isActive()) {
						logger.info("Campaign has become inactive: {}", camp.campaign.adId);
						camp.removeFromRTB();
						camp.report();
						parkCampaign(camp);
					} 		
				}
				
				List<String> additions = new ArrayList<String>();
				List<String> clist = new ArrayList<String>();
				for (String key : deletedCampaigns.keySet()) {
					AccountingCampaign camp = deletedCampaigns.get(key);
					if (camp.isRunnable()) {
						camp.runUsingElk();
						if (camp.isActive()) {
							logger.info("Currently inactive campaign going active: {}", camp.campaign.adId);
							campaigns.add(camp);
							additions.add(key);
							try {
								//camp.addToRTB();                                  // one at a time, not good. TBD
								clist.add(key);
							} catch (Exception error) {
								logger.error("Error: Failed to load campaign {} into bidders, reason: {}", camp.campaignid,error.toString());
							}
						}
					}

				}

				if (clist.size()>0)
					Configuration.getInstance().sendUpdateList(clist);
				
				for (String key : additions) {
					deletedCampaigns.remove(key);
				}

				/**
				 * Make sure any campaigns that have exceeded their total budgets or has expired are
				 * purged from crosstalk and the bidder.
				 */
				purge();

				Configuration.getInstance().executeListCampaigns();
				Configuration.getInstance().keepAlive();            // keep connection open to MySQL

				logger.info("Heartbeat,freedsk: {}, cpu: {}%, mem: {}, bidders: {},  runnable campaigns: {}, parked: {}, dailyspend: {} avg-spend-min: {}, deltaBids: {}",
						Performance.getPercFreeDisk(), Performance.getCpuPerfAsString(),
						Performance.getMemoryUsed(),
						Crosstalk.bidders.size(),
						this.campaigns.size(),this.deletedCampaigns.size(),
						budgets.getCampaignDailySpend(null),
						budgets.getCampaignSpendAverage(null),
						BidResponseHandler.getCountAndReset());
				Thread.sleep(60000);
			} catch (Exception error) {
				error.printStackTrace();
				if (error.toString().toLowerCase().contains("mysql")) {
					System.err.println("MySQL Error, goodbye");
					System.exit(0);
				}
			}
		}
	}

	@Override
	public void callBack(BasicCommand msg) {

	}

	void purge() throws Exception {
		String list = "";
		List<AccountingCampaign> dc = new ArrayList<AccountingCampaign>();
		for (String key : deletedCampaigns.keySet()) {
			AccountingCampaign c = deletedCampaigns.get(key);
			if (c.canBePurged()) {
				c.removeFromRTB();
				list += c.campaignid + " ";
				dc.add(c);
			}
		}

		for (AccountingCampaign c : dc) {
			deletedCampaigns.remove("" + c.campaignid);
		}

		for (AccountingCampaign c : campaigns) {
			if (c.canBePurged()) {
				c.removeFromRTB();
				list += c.campaignid + " ";
			}
		}

		if (list.length()>0) {
			logger.info("The following campaigns have been purged: {}",list);
		}
	}

	public static AccountingCampaign getActiveCampaign(String id) {
		for (AccountingCampaign camp : campaigns) {
			if (("" + camp.campaignid).equals(id))
				return camp;
		}
		return null;
	}

	/**
	 * Stop the bidders, the program has detected an error (likely SQL or
	 * network failure
	 * 
	 * @param reason
	 *            String. The reason to stop.
	 */
	public static void panicStopBidders(String reason)   {
		try {
		StopBidder stop = new StopBidder();
		stop.from = Configuration.getInstance().config.rtbName;
		Configuration.getInstance().sendCommand(false,stop);
		logger.error("PANIC STOP: " + reason);
			Configuration c = Configuration.getInstance();
			c.deadmanSwitch.deleteKey();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Park a campaign. This causes it to get unloaded from the bidders
	 * @param camp Campaign. The campaign to park.
	 * @return boolean. Returns true.
	 * @throws Exception if there was an error.
	 */
	boolean parkCampaign(AccountingCampaign camp) throws Exception {
		if (camp.campaign == null || deletedCampaigns.get(camp.campaign.adId) != null)
			return false;

		camp.removeFromRTB(); 						// sends the campaign
		deletedCampaigns.put(camp.campaign.adId, camp); // add to the deleted campaigns map
		campaigns.remove(camp);							// remove from the campaigns set, used on refresh
		return true;
	}

	/**
	 * Return a list of all the deleted (parked) campaigns
	 * 
	 * @return List. The deleted campaign.
	 */
	public List<AccountingCampaign> getDeletedCampaigns() {
		List<AccountingCampaign> list = new ArrayList<AccountingCampaign>();
		for (Map.Entry<String, AccountingCampaign> entry : deletedCampaigns.entrySet()) {
			list.add(entry.getValue());
		}
		return list;
	}

	public AccountingCampaign getCampaign(String campid) {
		for (AccountingCampaign camp : campaigns) {
			if (camp.campaign.adId.equals(campid))
				return camp;
		}

		AccountingCampaign camp = deletedCampaigns.get(campid);
		return camp;
	}

	///////////////////////////////////

	//////////////////////////////////// OVERLOAD ACCOUNTING
	//////////////////////////////////// /////////////////////////////////////////

	public static void handleLucene(AccountingCampaign camp) throws Exception {

		ResultSet rs;
		Configuration config = Configuration.getInstance();
		Statement stmt = config.getStatement();
		List<Integer> list = new ArrayList<Integer>();

		/////////////////////////////////////

		long now = new Date().getTime();
		PreparedStatement prep;
		String insert = "nothing";
		;
		try {

			Campaign cmp = camp.campaign;
			if (cmp != null) {
				for (int i = 0; i < cmp.creatives.size(); i++) {
					String crid = cmp.creatives.get(i).impid;
					String lucene = cmp.getLucene(crid);

					// System.out.println("Lucene: " + cmp.adId + "/" + crid +
					// "----> " + lucene);

					if (cmp.creatives.get(i).isVideo()) {
						insert = "select id from report_commands where campaign_id = " + camp.campaignid
								+ " AND banner_video_id = " + crid;
						prep = Configuration.getInstance().getConnection().prepareStatement(insert);
						rs = prep.executeQuery();
						if (rs.next()) {
							rs.getInt("id");
							insert = "update report_commands set  command=?,  updated_at=? where campaign_id = "
									+ camp.campaignid + " and banner_video_id = " + crid;
							prep = Configuration.getInstance().getConnection().prepareStatement(insert);
							prep.setString(1, lucene);
							prep.setTimestamp(2, new Timestamp(now));
						} else {
							insert = "insert into report_commands (campaign_id, banner_video_id, command, created_at, updated_at) values(?,?,?,?,?)";
							prep = Configuration.getInstance().getConnection().prepareStatement(insert);
							prep.setInt(1, Integer.parseInt(cmp.adId));
							prep.setInt(2, Integer.parseInt(crid));
							prep.setString(3, lucene);
							prep.setTimestamp(4, new Timestamp(now));
							prep.setTimestamp(5, new Timestamp(now));
						}
					} else {
						insert = "select id from report_commands where campaign_id = " + camp.campaignid
								+ " AND banner_id = " + crid;
						prep = Configuration.getInstance().getConnection().prepareStatement(insert);
						rs = prep.executeQuery();
						if (rs.next()) {
							rs.getInt("id");
							insert = "update report_commands set  command=?, updated_at=? where campaign_id = "
									+ camp.campaignid + " and banner_id = " + crid;
							prep = Configuration.getInstance().getConnection().prepareStatement(insert);
							prep.setString(1, lucene);
							prep.setTimestamp(2, new Timestamp(now));
						} else {
							insert = "insert into report_commands (campaign_id, banner_id, command, created_at, updated_at) values(?,?,?,?,?)";
							prep = Configuration.getInstance().getConnection().prepareStatement(insert);
							prep.setInt(1, Integer.parseInt(cmp.adId));
							prep.setInt(2, Integer.parseInt(crid));
							prep.setString(3, lucene);
							prep.setTimestamp(4, new Timestamp(now));
							prep.setTimestamp(5, new Timestamp(now));
						}
						prep.execute();
					}
				}
			}
		} catch (Exception error) {
			System.out.println("==========>" + insert);
			error.printStackTrace();
		}
	}
}
