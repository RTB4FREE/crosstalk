package com.jacamars.dsp.crosstalk.manager;

import java.io.File;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.jacamars.dsp.rtb.bidder.DeadmanSwitch;
import com.jacamars.dsp.rtb.commands.*;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.rtb.tools.HexDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jacamars.dsp.rtb.redisson.RedissonClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.crosstalk.api.ApiCommand;
import com.jacamars.dsp.crosstalk.api.WebAccess;
import com.jacamars.dsp.crosstalk.budget.BudgetController;
import com.jacamars.dsp.crosstalk.config.Aerospike;
import com.jacamars.dsp.crosstalk.config.App;
import com.jacamars.dsp.crosstalk.config.Config;
import com.jacamars.dsp.crosstalk.config.Elk;
import com.jacamars.dsp.crosstalk.config.Mysql;
import com.jacamars.dsp.crosstalk.config.Zeromq;
import com.mysql.jdbc.PreparedStatement;
import com.jacamars.dsp.rtb.bidder.Controller;
import com.jacamars.dsp.rtb.bidder.ZPublisher;

import com.jacamars.dsp.rtb.db.DataBaseObject;
import com.jacamars.dsp.rtb.jmq.MessageListener;
import com.jacamars.dsp.rtb.jmq.RTopic;


/**
 * Singleton configuration object, takes the config and sets up Elastic Search, MySQL and 0MQ.
 * @author Ben M. Faul
 *
 */
public enum Configuration {
	/** Instance object */
	CROSSTALK;

	/** Configuration object for the application */
	public Config config;

	/** The jackson json parser object */

	static ObjectMapper mapper = new ObjectMapper();

	/** Redisson configuration object */
	public RedissonClient redisson;

	/** Win response object */
	RTopic winners;

	/** Command response object */
	RTopic responses;


	/** 0MQ topic for requests */
	RTopic requests;

	/** 0MQ topic for clicks and pixels */
	RTopic clicks;

	/** 0MQ topic for bids */
	RTopic bids;

	/** Where the rtb4free admin logs are */
	ZPublisher rtblogs;

	/** Unified logger object */
	static UnifiedLogger unilogger;

	/** Publisher for 0MQ commands to bidders */
	static volatile ZPublisher commandsQueue;

	/** Publisher for the performance logs */
	static public volatile ZPublisher perfLogs;

	/** The redisson backed shared map that represents the database */
	public DataBaseObject shared;
	// private Redisson mapRedisson;

	/** The MySQL connection */
	Connection connect = null;

	/* An SQL statement object */
	Statement statement = null;

	/** This class's sl4j logger object */
	static final Logger logger = LoggerFactory.getLogger(Configuration.class);

	public volatile DeadmanSwitch deadmanSwitch;
	/** The deadman switch key in Aerospike */
	public String deadmanKey = "accountingsystem";

	/** If specified, only these campaigns may receive api calls */
	public static final Set<String> apiAcl = new HashSet();

	/**
	 * Return the instance of the configuration, after it was iniitalized
	 * @return Configuration. The instance that was configured
	 */
	public static Configuration getInstance() {
		return CROSSTALK;
	}

	/**
	 * Connect to SQL.
	 * @throws Exception on network or SQL access errros
	 */
	public void connectToSql() throws Exception {
		/**
		 * Now connect to SQL
		 */
		// load the MySQL driver
		Class.forName("com.mysql.jdbc.Driver");

		// Get the connection
		try {
			DriverManager.setLoginTimeout(20);
			connect = DriverManager.getConnection(config.mysql.getLogin());

		} catch (Exception error) {
			logger.error("Can't connect to MySQL, error: {}",error.getMessage());
			throw error;
		}

		// Get the statement handler
		statement = connect.createStatement();
		statement.execute("delete from report_commands");
	}

	/**
	 * Reconnect to MySQL.
	 * @throws Exception on connection errors.
	 */
	public void reconnectMySql() throws Exception {
		try {
			connect.close();
			connectToSql();
		} catch (Exception error) {

		}
	}

	/**
	 * A cheap call to MySQL so the connection stays open. Dumbass MySQL.
	 * @throws Exception on MySQL Errors.
	 */
	public void keepAlive() throws Exception {
		connect.getMetaData().getCatalogs();
	}

	/**
	 * Write the map
	 */
	static long zulu = 0;


	public void initialize(String in, ScannerIF commandResponses, ScannerIF wins) throws Exception {
		String content = new String(Files.readAllBytes(Paths.get(in)));
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		config = mapper.readValue(content, Config.class);

		config.fixRegion();

		if (commandResponses == null || wins == null)
			return;

		logger.info("REGION is set to {}",config.region);
		logger.info("Global ELK is set to {}, Regional ELK is set to {}, port: {}",config.elk.getAggHost(),config.elk.getHost(), config.elk.getPort());

		Scanner.budgets = null;
		if (config.elk.simFile != null)
			Scanner.budgets = BudgetController.getInstance(config.elk.simFile);
		else
			Scanner.budgets = BudgetController.getInstance(config.elk.getHost(),config.elk.getAggHost(),config.elk.getPort());

		int k = 0;
		while(k < 10 && Scanner.budgets.getRevolution() == 0) {
			Thread.sleep(1000);
		}
		if (Scanner.budgets.getRevolution() == 0) {
			logger.error("*** ERROR: Could not establish budgets from: " + config.elk.getHost());
			System.exit(0);
		}

		Zeromq zmq = Configuration.getInstance().config.zeromq;
		String COMMANDS = com.jacamars.dsp.rtb.common.Configuration.substitute(zmq.commands);
		String RESPONSES = com.jacamars.dsp.rtb.common.Configuration.substitute(zmq.responses);
		String ls = com.jacamars.dsp.rtb.common.Configuration.substitute(zmq.xfrport);
		int listen = Integer.parseInt(ls);
		String host = com.jacamars.dsp.rtb.common.Configuration.getHostFrom(COMMANDS);
		int pub  = com.jacamars.dsp.rtb.common.Configuration.getPortFrom(COMMANDS);
		int sub = com.jacamars.dsp.rtb.common.Configuration.getPortFrom(RESPONSES);

		String str = com.jacamars.dsp.rtb.common.Configuration.substitute(COMMANDS);
		logger.info("Commands will be sent to: {}",str);
		commandsQueue = new ZPublisher(str);

		logger.info("Responses are sent: {}, Responses Received: {}, Redisson host: {}, Redisson pub: {}, Redisson sub: {}, Redisson init: {}",
				COMMANDS, RESPONSES, host, pub, sub, listen);

		redisson = new RedissonClient();

		redisson.setSharedObject(host,pub,sub,listen);
		shared = DataBaseObject.getInstance(redisson);

		responses = new RTopic(RESPONSES);
		responses.addListener(new MessageListener<BasicCommand>() {
			@Override
			public void onMessage(String channel, BasicCommand msg) {
				try {
					BidderProtocol.processAck(msg);
					if (msg.from.startsWith("crosstalk"))
						return;

					if (msg.cmd == Controller.SHUTDOWNNOTICE) {
						if (Crosstalk.tracker != null)
							Crosstalk.tracker.remove(msg.from);
						logger.warn("This bidder has sent a shutdown notice: {}", msg.from);
						return;
					}

					if (msg.to.startsWith(WebAccess.uuid)) {
						ApiCommand.callBack(msg);
						return;
					}

					if (Crosstalk.node != null)
						Crosstalk.node.addMember(msg.from);

					String content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(msg);
					// System.out.println("<------" + content);
					logger.debug(
							"Bidder(s) sent command response: {}, id: {}", msg.toString(), msg.id);


					if (msg.cmd == Controller.LIST_CAMPAIGNS_RESPONSE) {
						Configuration.getInstance().checkRunning(msg);
						return;
					}

					if (msg.cmd == Controller.ECHO) {
						int count = 0;
						while (Crosstalk.tracker == null && count < 15) {
							Thread.sleep(1000);
							count++;
						}
						if (Crosstalk.node == null) {
							logger.warn("Campaigns:node:error: node processor is down");
							return;
						}

						Crosstalk.tracker.checkFor(msg);
					}
					commandResponses.callBack(msg);
				} catch (Exception error) {
					error.printStackTrace();
				}
			}
		});

		new WebAccess(config.app.getPort());

		if (config.app.apiAcl != null) {
			apiAcl.addAll(config.app.apiAcl);
		}
		logger.info("Web API is available on port: {}",config.app.getPort());

		deadmanSwitch = new DeadmanSwitch(redisson,deadmanKey);

	}

	public void checkRunning(BasicCommand c) throws Exception {

		if (c.from == null) {
			System.out.println("SUSPECT COMMAND: " + c);
			return;
		}
		String [] parts = c.target.split(" ");
		List<String> theirs = new ArrayList<String>();
		for (String p : parts) {
			if (p.length() > 0)
				theirs.add(p);
		}
		List<String> mine = new ArrayList<String>();
		for (AccountingCampaign camp : Scanner.campaigns) {
			mine.add(camp.campaign.adId);
		}

		if (BudgetController.revolution > 2) {
			logger.info("My campaigns[{}]: {}", mine.size(),mine);
			logger.info("Remote: {}, campaigns[{}]: {}", c.from,theirs.size(),theirs);

			ArrayList<String> remove = new ArrayList<String>(theirs);
			remove.removeAll(mine);
			if (remove.size()>0) {
				logger.warn("Leftover campaigns found in bidder: {}", remove);
				fixDeletedCampaigns(remove);
			}

			ArrayList<String> add = new ArrayList<String>(mine);
			add.removeAll(theirs);
			if (add.size()>0) {
				logger.warn("Bidder {} is missing these campaigns: {}",c.from,add);
				fixMissingInBidder(c.from,add);
			}

			for (String r : remove) {
				if (r != "") {
					DeleteCampaign del = new DeleteCampaign(config.rtbName, r);
					del.from = Configuration.getInstance().config.rtbName;
					del.id = UUID.randomUUID().toString();
					del.to = "*";
					sendCommand(true, del);
				}
			}
		}
	}

	public String listSymbols(String id) throws Exception {
		ListSymbols ls = new ListSymbols();
		ls.from = WebAccess.uuid;
		ls.id = id;
		ls.to = "*";
		sendCommand(true, ls);
		return ls.id;
	}
	
	
	/**
	 * For some reason, campaigns are missing from a bidder.... Fix it.
	 * @param whom String. Who it is missing from.
	 * @param what List. The list of campaigns.
	 */
	void fixMissingInBidder(String whom, List<String> what) {
		for (String cid : what) {
			logger.warn("Patching in missing campaign: {}",cid);
			AccountingCampaign campaign = Scanner.getActiveCampaign(cid);
			try {
				campaign.patchin();
				Thread.sleep(500);
				campaign.addToRTB();
				Thread.sleep(1000);
			} catch (Exception error) {
				logger.error("Error updating campaign: {}, error: {}",cid,error);
			}
		}
	}

	/**
	 * For some reason, a bidder has left over campaigns. Get rid of them.
	 * @param what List. The list to remove.
	 */
	void fixDeletedCampaigns(List<String> what) {
		for (String cid : what) {
			logger.warn("Deleting dangling campaign: {}", cid);
			HexDump.dumpHexData(System.out,"CID",cid.getBytes(),cid.getBytes().length);
			AccountingCampaign campaign = Scanner.deletedCampaigns.get(cid);
			if (campaign != null) {
				try {
					campaign.removeFromRTB();
					Thread.sleep(1000);
				} catch (Exception error) {
					logger.error("Error updating campaign: {}, error: {}", cid, error);
				}
			} else {
				logger.error("Dangling campaign: {} could not be deleted", cid);
			}
		}
	}

	public List<String> getAddresses(List<String> hosts, String channel) {
		List<String> addrs = new ArrayList<String>();
		for (String host : hosts) {
			if (host.startsWith("kafka"))
				return hosts;
			String address = "tcp://" + host + ":" + channel;
			addrs.add(address);
		}
		return addrs;
	}

	public void executeListCampaigns() {
		ListCampaigns cmd = new ListCampaigns();
		cmd.from = Configuration.getInstance().config.rtbName;
		commandsQueue.add(cmd);
	}

	@JsonIgnore
	public Statement getStatement() {
		return statement;
	}

	public void sendCommand(boolean watch, BasicCommand cmd) throws Exception {
		String to = cmd.to;
		if (cmd.from == null || cmd.from.startsWith("crosstalk")==false)
			cmd.from = Configuration.getInstance().config.rtbName;

		if (to.equals("crosstalk")) {
		    logger.error("Can't send a message to yourself!. name: {}. cmd.id: {}, cmd.target: {}", cmd.name, cmd.id, cmd.target);
		    throw new Exception("Can't send message to yourself");
        }
		if (to.equals(""))
			to = "*";

		cmd.timestamp = System.currentTimeMillis();
		String name = "" + cmd.cmd;

		if (cmd.cmd == 0)
			name = "ADD CAMPAIGN";
		if (cmd.cmd == 1)
			name = "DELETE CAMPAIGN";
		if (cmd.cmd == 2)
			name = "STOP BIDDERS";
		if (cmd.cmd == 3)
			name = "START BIDDERS";
		if (cmd.cmd == Controller.SET_WEIGHTS)
			name = "SET WEIGHTS";
        if (cmd.cmd == Controller.GET_WEIGHTS)
            name = "GET WEIGHTS";

		logger.info("---------> {}:{}: {}/{}", name, cmd.id, cmd.name,cmd.target);

		commandsQueue.add(cmd);
		if (watch)
			BidderProtocol.process(cmd);
	}

	/**
	 * Send all known campaigns at once to the bidder
	 * @param name String. The bidder to send to.
	 * @param campaigns Set. The set of campaigns to send.
	 */
	public List<String> sendCampaignList(String name, Set<AccountingCampaign> campaigns) throws Exception {
		String list = "";
		List<String> camps = new ArrayList<String>();
		for (AccountingCampaign camp : campaigns) {
			list += camp.campaignid + ",";
			camps.add(""+camp.campaignid);
		}
		if (list.length()==0)
			return camps;

		list = list.substring(0,list.length()-1);
		AddCampaignsList cmd = new AddCampaignsList(name, list);
		sendCommand(true,cmd);
		return camps;
	}

	@JsonIgnore
	public App getApp() {
		return config.app;
	}

	@JsonIgnore
	public Elk getElk() {
		return config.elk;
	}

	@JsonIgnore
	public Aerospike getAerospike() {
		return config.aerospike;
	}

	@JsonIgnore
	public Mysql getMysql() {
		return config.mysql;
	}

	@JsonIgnore
	public Connection getConnection() {
		return connect;
	}

	@JsonIgnore
	public RedissonClient getRedisson() {
		return redisson;
	}

	public void sendUpdateList(List<String> list) throws Exception {
		String target = "";
		String x = null;
		for (int i = 0; i < list.size() - 1; i++) {
			x = list.get(i);
			target += x + ",";
		}
		x = list.get(list.size() - 1);
		target += x;
		AddCampaignsList cmd = new AddCampaignsList();
		cmd.to = "*";
		cmd.target = target;
		Configuration.getInstance().sendCommand(true,cmd);
	}

	public static String getPercentFree() throws Exception {

		File file = new File("/");

		long totalSpace = file.getTotalSpace(); // total disk space in bytes.
		long usableSpace = file.getUsableSpace(); /// unallocated / free disk
													/// space in bytes.
		long freeSpace = file.getFreeSpace(); // unallocated / free disk space
												// in bytes.

		double percent = (double) freeSpace / (double) totalSpace * 100;
		String str = "" + percent;
		str = str.substring(0, str.indexOf(".") + 3) + "%";
		return str;
	}
}
