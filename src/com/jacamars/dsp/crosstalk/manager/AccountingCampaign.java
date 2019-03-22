package com.jacamars.dsp.crosstalk.manager;


import com.jacamars.dsp.rtb.commands.AddCampaign;
import com.jacamars.dsp.rtb.commands.DeleteCampaign;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.rtb.common.Creative;
import com.jacamars.dsp.rtb.common.FrequencyCap;
import com.jacamars.dsp.rtb.common.Node;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.tools.ResultSetToJSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * 
 * A class that builds the RTB4FREE representation of a campaign described in MySQL. A JSON representation of the MySQL definition of the
 * campaign that was defined in MySQL is converted to the RTB4FREE form. Also, the budgets and spends are monitored in this object. The object
 * also contains the Target object and an array of Creatives that implement the campaign.
 * 
 * @author Ben M. Faul
 *
 */
public class AccountingCampaign {

	/** The owner of this campaign, the 'user' if you will. Not really useful, is a holdover from previous versions. */
	protected String owner;

	/**  The campaign id for this campaign. This is the database key. */
	public int campaignid;

	/** The total budget for this campaign */
	protected volatile AtomicBigDecimal total_budget = new AtomicBigDecimal(0);

	/** The current cost incurred by this campaign */
	protected volatile AtomicBigDecimal cost = new AtomicBigDecimal(0); 

	/** A flag from MySQL stating the campaign was updated */
	public long updated;
	
	/** The date time this campaign will be active */
	public long activate_time;
	
	/** The date time this campaign will expire */
	public long expire_time;
	
	/** All of the creatives of this campaign */
	protected Set<AccountingCreative> creatives = new HashSet<AccountingCreative>();
	
	/** The creatives that are inactive for whatever reason */
	protected Set<AccountingCreative> parkedCreatives = new HashSet<AccountingCreative>();

	/** The targeting object used with this campaign */
	protected Targeting targeting = null;

	/** The campaign descriptive name */
	
	protected String campaignname = "";
	
	/** The ad domain of this campaign */
	protected String adomain = "rtb4free.com";

	/** The RTB4FREE campaign that was generated from this object */
	public Campaign campaign = null;
	
	/** The JSON Object of this campaign that represents the SQL */
	private ObjectNode myNode = null;

	/** The daily budget of this campaign */
	AtomicBigDecimal dailyBudget = null;
	
	/** The hourly budget of this campaign */
	AtomicBigDecimal hourlyBudget = null;
	
	/** The current daily cost */
	protected AtomicBigDecimal dailyCost = new AtomicBigDecimal(0);
	
	/** The current hourly cost */
	protected AtomicBigDecimal hourlyCost = new AtomicBigDecimal(0);

	/** The SQL name for this campaign id */
	protected final String CAMPAIGN_ID = "id";
	
	/** Thew SQL name for the updated flag */
	protected final String UPDATED = "updated_at";
	
	/** The SQL name for the total budget */
	protected final String TOTAL_BUDGET = "total_budget";
	
	/** SQL name for the descriptive name */
	protected final String CAMPAIGN_NAME = "name";
	
	/** SQL name of Datetime of expiration */
	protected final String EXPIRE_TIME = "expire_time";
	
	/** SQL name for the date time to activate */
	protected final String ACTIVATE_TIME = "activate_time";
	
	/** The SQL name for the budget limit daily */
	protected final String DAILY_BUDGET = "budget_limit_daily";
	
	/** The SQL name for the hourly budget */
	protected final String HOURLY_BUDGET = "budget_limit_hourly";

	protected final String DAYPART = "day_parting_utc";

	/** Whether or not to use forensiq. Not used */
	String forensiq;
	
	/** The exchanges this campaign can be used with */
	protected List<String> exchanges = new ArrayList<String>();

	/** blocked categories */
	protected List<String> bcat = new ArrayList<String>();

	/** If set to runnable then this will be allowed into the bidders (if budget is ok and not expired. Anything else means no loading */
	protected String status;

	/** This class's sl4j logger */
	static final Logger logger = LoggerFactory.getLogger(AccountingCampaign.class);
	
	/** The frequency capping specification, as in device.ip */
	String capSpec;
	
	/** Number of seconds before the frequency cap expires */
	int capExpire;
	
	/** The count limit of the frequency cap */
	int capCount;

	/** cap time unit **/
	String capTimeUnit;

	/** The day part structure */
	DayPart daypart;

	/**
	 * Default constructor.
	 */
	public AccountingCampaign() {

	}
	
	public AccountingCampaign(ObjectNode myNode) throws Exception {
		this.setMyNode(myNode);
		setup();
		process();
		doTargets();
		updateSpike();
	}

	public void stop() {
		for (AccountingCreative c : creatives) {
			c.stop();
		}
	}

	public void update(ObjectNode myNode) throws Exception {
		this.setMyNode(myNode);
		this.creatives.clear();
		this.bcat.clear();
		this.exchanges.clear();

		setup();
		process();
		doTargets();
		updateSpike();
	}

	public void runUsingElk() {
		try {

			cost.set(Scanner.budgets.getCampaignTotalSpend("" + campaignid));
			dailyCost.set(Scanner.budgets.getCampaignDailySpend("" + campaignid));
			hourlyCost.set(Scanner.budgets.getCampaignHourlySpend("" + campaignid));

			logger.debug("*** ELK TEST: Updating budgets CAMPAIGN:{}", campaignid);
			logger.debug("Total cost: {}, daily cost: {}, hourly cost: {}", cost.getDoubleValue(),
					dailyCost.getDoubleValue(), hourlyCost.getDoubleValue());

			for (AccountingCreative c : creatives) {
				c.runUsingElk();
			}
			process();

		} catch (Exception error) {
			error.printStackTrace();
		}
	}

	// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	// Override to create your own version of this
	protected void setup() throws Exception {
		campaignid = getMyNode().get(CAMPAIGN_ID).asInt();
		cost = new AtomicBigDecimal(getMyNode().get("cost").asDouble());
		dailyCost = new AtomicBigDecimal(getMyNode().get("daily_cost").asDouble(0.0));
		hourlyCost = new AtomicBigDecimal(getMyNode().get("hourly_cost").asDouble(0.0));
		expire_time = getMyNode().get(EXPIRE_TIME).asLong();
		activate_time = getMyNode().get(ACTIVATE_TIME).asLong();
		total_budget = new AtomicBigDecimal(getMyNode().get(TOTAL_BUDGET).asDouble());

		if (getMyNode().get(DAYPART) != null && getMyNode().get(DAYPART) instanceof MissingNode == false) {
				String parts = getMyNode().get(DAYPART).asText();
				if (parts.equals("null") || parts.length()==0)
					daypart = null;
			else
				daypart = new DayPart(parts);
		} else
			daypart = null;

		if (getMyNode().get("bcat") != null) {
			String str = getMyNode().get("bcat").asText();
			if (str.trim().length() != 0) {
				if (str.equals("null")==false)
					Targeting.getList(bcat, str);
			}
		}

		if (getMyNode().get("exchanges") != null && getMyNode().get("exchanges").asText().length() != 0) {
			exchanges.clear();
			String str = getMyNode().get("exchanges").asText(null);
			if (str != null) {
				Targeting.getList(exchanges, str);
			}
		}

		Object x = getMyNode().get(DAILY_BUDGET);
		if (x != null && !(x instanceof NullNode)) {
			if (dailyBudget == null || (dailyBudget.doubleValue() != getMyNode().get(DAILY_BUDGET).asDouble())) {
				dailyBudget = new AtomicBigDecimal(getMyNode().get(DAILY_BUDGET).asDouble());
			}
		} else
			dailyBudget = null;

		x = getMyNode().get(HOURLY_BUDGET);
		if (x != null && !(x instanceof NullNode)) {
			if (hourlyBudget == null || (hourlyBudget.doubleValue() != getMyNode().get(HOURLY_BUDGET).asDouble())) {
				hourlyBudget = new AtomicBigDecimal(getMyNode().get(HOURLY_BUDGET).asDouble());
			}
		} else
			hourlyBudget = null;

		x = getMyNode().get("targetting");
		if (x instanceof NullNode) {
			if (!isActive())
				return;
			throw new Exception("Can't have null targetting for campaign " + campaignid);
		}

		owner = Configuration.getInstance().config.rtbName;
		campaign = new Campaign();
		campaign.owner = owner;
		campaignid = getMyNode().get(CAMPAIGN_ID).asInt();
		campaign.adId = "" + campaignid;
		updated = getMyNode().get(UPDATED).asLong(0);

		// update_time = rs.getTimestamp("update_time"); ???????????? Is this
		// the trigger
		campaignname = getMyNode().get(CAMPAIGN_NAME).asText();
		adomain = getMyNode().get("ad_domain").asText();

	}

	public double costAsDouble() {
		return cost.doubleValue();
	}


	/**
	 * Set the new total budget. Used by the api.
	 * @param amount double. The value to set.
	 */
	public void setTotalBudget(double amount) {
		total_budget.set(amount);
	}

	/**
	 * Set the new total daily. Used by the api.
	 * @param amount double. The value to set.
	 */
	public void setDailyBudget(double amount) {
		this.dailyBudget.set(amount);
	}

	/**
	 * Set the new hourly budget. Used by the api.
	 * @param amount double. The value to set.
	 */
	public void setHourlyBudget(double amount) {
		this.hourlyBudget.set(amount);
	}

	protected void doTargets() throws Exception {
		if (getMyNode().get("targetting") instanceof NullNode) {
			if (!isActive())
				return;
			throw new Exception("No targeting record was found. for " + campaignid);
		}

		ObjectNode targ = (ObjectNode) getMyNode().get("targetting");
		targeting = new Targeting(this, targ);

		instantiate("banner", true);
		instantiate("banner_video", false);
		compile();
	}

	/**
	 * Call after you compile!
	 * 
	 * @throws Exception on JSON errors.
	 */
	void doStandardRtb() throws Exception {

		ArrayNode array = ResultSetToJSON.factory.arrayNode();
		for (int i = 0; i < Scanner.campaignRtbStd.size(); i++) {
			JsonNode node = Scanner.campaignRtbStd.get(i);
			if (campaignid == node.get("campaign_id").asInt()) {
				Integer key = node.get("rtb_standard_id").asInt();
				JsonNode x = Scanner.gloablRtbSpecification.get(key);
				array.add(x);
			}
		}
		RtbStandard.processStandard(array, campaign.attributes);
	}

	// Override this to match your code.
	public boolean changed(ObjectNode node) throws Exception {

		long updated = node.get("updated_at").asLong();
		if (this.updated != updated)
			return true;

		return false;
	}

	protected AccountingCreative getCreative(String id) {
		//
		// Check the active creatives
		//
		for (AccountingCreative c : creatives) {
			if (c.bannerid.equals(id))
				return c;
		}

		//
		// Check parked creatives
		//
		for (AccountingCreative c : parkedCreatives) {
			if (c.bannerid.equals(id))
				return c;
		}
		return null;
	}

	protected AccountingCreative getCreative(ObjectNode node) {
		String id = node.get("id").asText(); ///////////////// here
		return getCreative(id);
	}

	/**
	 * Instantiate a creative.
	 * 
	 * @param type
	 *            String. The type of creative.
	 * @param isBanner
	 *            boolean. Is this a banner
	 * @throws Exception on SQL or JSON errors.
	 * 
	 */
	protected void instantiate(String type, boolean isBanner) throws Exception {

		ArrayNode array = (ArrayNode) getMyNode().get(type);
		if (array == null)
			return;

		for (int i = 0; i < array.size(); i++) {
			ObjectNode node = (ObjectNode) array.get(i);
			AccountingCreative creative = new AccountingCreative(node, isBanner); // Mobi!
			if (!(creative.budgetExceeded())) {
				unpark(creative);
				Scanner.creativesMap.put("" + campaignid + ":" + creative.bannerid, creative);
			} else {
				park(creative);
			}
		}
	}

	/**
	 * Report why the campaign is not runnable.
	 * @return String. The reasons why...
	 * @throws Exception on ES errors.
	 */
	public String report() throws Exception {
		if (campaignid == 1499) {
			System.out.println("HERE");
		}
		String reason = "";
		if (!isRunnable())
			reason = "Marked not runnable.";
		if (budgetExceeded()) {
			if (reason.length() != 0)
				reason += " ";
			if (Scanner.budgets.checkCampaignBudgetsTotal(""+campaignid, total_budget))
				reason += "Campaign total cost: " + cost.getDoubleValue() + " exceeds: " + total_budget.getDoubleValue() + ". ";
			if (Scanner.budgets.checkCampaignBudgetsDaily(""+campaignid, dailyBudget))
				reason += "Campaign daily cost: " + dailyCost.getDoubleValue() + " exceeded: " + dailyBudget.getDoubleValue() + ". ";
			if (Scanner.budgets.checkCampaignBudgetsHourly(""+campaignid, hourlyBudget))
				reason += "Campaign hourly cost: " + hourlyCost.getDoubleValue() + " exceeded: " + hourlyBudget.getDoubleValue() + ". ";

		}

		if (isExpired()) {
			if (reason.length() > 0)
				reason += " ";
			reason += "Bid window closed, expiry. ";
		} else if (!budgetExceeded()) {
			if (reason.length() > 0)
				reason += " ";

			if (daypart != null) {
				if (daypart.isActive() != true) {
					reason += "Daypart is not active ";
				}
			}
			else
			if (targeting == null)
				reason += "No targeting. ";
			else
			if (creatives.size() == 0)
				reason += "No creatives attached. ";
		}

		List<Map> xreasons = new ArrayList<Map>();
		if (creatives.size() != 0) {
			for (AccountingCreative p : parkedCreatives) {
				Map<String, Object> r = new HashMap<String, Object>();
				r.put("creative",p.bannerid);
				List<String> reasons = new ArrayList<String>();
				if (p.budgetExceeded()) {
					reasons.add("nobudget");
				}

				r.put("reasons",reasons);
			}
		}

		if (xreasons.size() != 0) {
			reason += Configuration.mapper.writeValueAsString(xreasons);
		}
		if (reason.length() > 0)
			logger.info("Campaign {} not loaded: {}",campaignid, reason);

		if (reason.length() == 0)
			reason = "Runnable";
		return reason;
	}

	public boolean process() throws Exception {
		boolean change = false;
		int n = creatives.size();
		List<AccountingCreative> list = new ArrayList<AccountingCreative>();

		for (AccountingCreative c : parkedCreatives) {
			if (!c.budgetExceeded()) {
				unpark(c);
				change = true;
			}
		}

		for (AccountingCreative creative : creatives) {
			if (creative.budgetExceeded()) {
				list.add(creative);
				change = true;
			}
		}

		for (AccountingCreative c : list) {
			park(c);
		}

		if (creatives.size() != n)
			change = true;

		if (myNode.get("frequency_spec") != null) {
			capSpec = myNode.get("frequency_spec").asText(null);
		}
		if (capSpec != null && capSpec.length() != 0) {
			capCount = myNode.get("frequency_count").asInt(0);
			capExpire = myNode.get("frequency_expire").asInt();
			capTimeUnit = myNode.get("frequency_interval_type").asText();
		}

		return change;
	}

	protected void park(AccountingCreative c) {
		creatives.remove(c);
		parkedCreatives.add(c);
	}

	protected void unpark(AccountingCreative c) {
		parkedCreatives.remove(c);
		creatives.add(c);
	}

	public boolean isExpired() {
		Date date = new Date();
		boolean expired = date.getTime() > expire_time;
		if (expired)
			return expired;
		expired = date.getTime() < activate_time;
		if (expired)
			return expired;
		return false;

	}

	public void setStatus(String status) {
		myNode.put("status",status);
	}

	public boolean isRunnable() throws Exception {
		String status = getMyNode().get("status").asText("undefined");
		if (!status.equals("runnable"))
			return false;
		return true;
	}

	public boolean isActive() throws Exception {
		Date date = new Date();
		if (!isRunnable())
			return false;

		if (creatives.size() == 0) {
			return false;
		}

		if (budgetExceeded()) {
			logger.debug("BUDGET EXCEEDED: {}", this.campaignid);
			return false;
		}

		if ((date.getTime() >= activate_time) && (date.getTime() <= expire_time)) {

			if (daypart != null) {
				if (daypart.isActive() != true) {
					logger.debug("Daypart is not active: {}", this.campaignid);
					return false;
				}
			}

			logger.debug("IS ACTIVE: {}", this.campaignid);
			return true;
		} else {
			logger.debug("ACTIVATION TIME NOT IN RANGE: {}", campaignid);
			return false;
		}
	}

	public long getUpdate() {
		return updated;
	}

	public boolean budgetExceeded() throws Exception {
		if (Scanner.budgets == null)
			return false;

		return Scanner.budgets.checkCampaignBudgets(""+campaignid, total_budget, dailyBudget, hourlyBudget);
	}

	public void removeFromRTB() throws Exception {
		Configuration config = Configuration.getInstance();
		// remove from bidders!
		DeleteCampaign cmd = new DeleteCampaign(Configuration.getInstance().config.rtbName, "" + campaignid);
		cmd.from = Configuration.getInstance().config.rtbName;
		cmd.to = "*";
		cmd.id = UUID.randomUUID().toString();
		config.sendCommand(true,cmd);

		Scanner.campaigns.remove(this);
	}

	public boolean compareTo(AccountingCampaign t) {
		return false;
	}

	public void compile() throws Exception {
		campaign.adomain = adomain;
		campaign.name = campaignname;
		campaign.creatives.clear();
		for (AccountingCreative c : creatives) {
			if (!c.budgetExceeded())
				campaign.creatives.add(c.compile());
		}
		if (forensiq != null) {
			if (forensiq.equals("Y") || forensiq.equals("y"))
				campaign.forensiq = true;
			else
				campaign.forensiq = false;
		}

		List<Node> nodes;
		campaign.attributes.clear();
		if (targeting != null) {
			nodes = targeting.compile();
			if (nodes != null) {
				for (Node n : nodes)
					campaign.attributes.add(n);
			}
		}

		if (exchanges.size() != 0) {
			Node n = new Node("exchanges", "exchange", Node.MEMBER, exchanges);
			n.notPresentOk = false;
			campaign.attributes.add(n);
		}

		if (bcat.size() != 0) {
			Node n = new Node("bcat", "bcat", Node.NOT_INTERSECTS, bcat);
			n.notPresentOk = true;
			campaign.attributes.add(n);
		}

		int k = 0;
		for (Creative c : this.campaign.creatives) {
			if (c.adxCreativeExtensions != null)
				k++;
		}
		if (k > 0)
			this.campaign.isAdx = true;
		else
			this.campaign.isAdx = false;

		if (capSpec != null && capSpec.length() > 0 && capCount > 0 && capExpire > 0) {
			campaign.frequencyCap = new FrequencyCap();
			campaign.frequencyCap.capSpecification = new ArrayList<String>();
			Targeting.getList(campaign.frequencyCap.capSpecification, capSpec);
			campaign.frequencyCap.capTimeout = capExpire; // in seconds
			campaign.frequencyCap.capFrequency = capCount;
			campaign.frequencyCap.capTimeUnit = capTimeUnit;
		}
		doStandardRtb();
	}

	/**
	 * Adds this campaign to the RTB bidders.
	 * 
	 * @return boolean. Returns true if it was added (that it has creatives)/
	 * @throws Exception
	 *             on JSON errors.
	 */
	public boolean updateSpike() throws Exception {

		if (Scanner.budgets == null)
			return false;
		
		if (creatives.size() == 0) { // if there are no effective creatives,							// don't add it!
			return false;
		}

		Configuration config = Configuration.getInstance();
		List<Campaign> campaigns = config.shared.getCampaigns();

		for (int i = 0; i < campaigns.size(); i++) {
			Campaign x = campaigns.get(i);
			if (x.adId.equals(campaign.adId)) {
				campaigns.remove(i);
				break;
			}
		}

		campaigns.add(campaign);
		Scanner.campaigns.add(this);

		Configuration.getInstance().shared.putCampaigns(campaigns);

		for (AccountingCreative creat : creatives) {
			Scanner.creativesMap.put(campaign.adId + ":" + creat.bannerid, creat);
		}

		return true;
	}
	
	/**
	 * Patch the campaign back into campaigns, in case zerospike is missing it.
	 * @throws Exception on shared access errors
	 */
	public void patchin() throws Exception {
		Configuration config = Configuration.getInstance();
		List<Campaign> campaigns = config.shared.getCampaigns();

		for (int i = 0; i < campaigns.size(); i++) {
			Campaign x = campaigns.get(i);
			if (x.adId.equals(campaign.adId)) {
				campaigns.remove(i);
				break;
			}
		}

		campaigns.add(campaign);
		Scanner.campaigns.add(this);

		Configuration.getInstance().shared.putCampaigns(campaigns);
	}

	/**
	 * Check and see if this campaign is deletable fron the system
	 * @return boolean. If campaign is expired or the total spend has been reached.
	 * @throws Exception on errors computing budgets.
	 */
	public boolean canBePurged() throws Exception {
		if (isExpired())
			return true;
		return Scanner.budgets.checkCampaignTotalBudgetExceeded(""+this.campaignid, total_budget);
	}

	public boolean addToRTB() throws Exception {
		transmitAdd();
		return true;
	}

	public boolean addToRTB(String bidder) throws Exception {
		transmitAdd(bidder);
		return true;
	}

	public Set<AccountingCreative> getCreatives() {
		return creatives;
	}

	/**
	 * Get the actual RTB campaign that will be loaded into the bidder.
	 * 
	 * @return Campaign. An RTB campaign, in JSON form.
	 * @throws Exception
	 *             on JSON errors.
	 */
	public Campaign getRTB() throws Exception {
		compile();
		if (targeting != null)
			targeting.compile();
		return campaign;
	}

	/**
	 * Transmit the campaign to the appropriate RTB bidders
	 */
	void transmitAdd() throws Exception {

		/* Tell the bidders to load into memory */
		String name = Configuration.getInstance().config.rtbName;
		AddCampaign cmd = new AddCampaign(name, "" + campaignid);
		cmd.from = UUID.randomUUID().toString();
		cmd.to = "*";


		Configuration.getInstance().sendCommand(true,cmd);
	}

	/**
	 * Transmit the campaign to the appropriate RTB bidders
	 * @param bidder String. The bidder to transmit the command to.
	 */
	void transmitAdd(String bidder) throws Exception {

		/* Tell the bidders to load into memory */
		String name = Configuration.getInstance().config.rtbName;
		AddCampaign cmd = new AddCampaign(name, "" + campaignid);
		cmd.from = UUID.randomUUID().toString();
		cmd.to = bidder;
		Configuration.getInstance().sendCommand(true,cmd);
	}

	/**
	 * Return the string representation of the RTB4FREE campaign as it would exist in Aerospike or the bidder.
	 * @return String. The JSON string of the campaign.
	 */
	public String getOutput() {
		return this.campaign.toJson();
	}

	/**
	 * Return my JSON node that represents the SQL for this.
	 * @return ObjectNode. The jackson JSON object of this campaign as it exists in SQL.
	 */
	public ObjectNode getMyNode() {
		return myNode;
	}

	/**
	 * Set the my node object to a new JSON object.
	 * @param myNode ObjectNode. The new jackson object to use.
	 */
	public void setMyNode(ObjectNode myNode) {
		this.myNode = myNode;
	}

}
