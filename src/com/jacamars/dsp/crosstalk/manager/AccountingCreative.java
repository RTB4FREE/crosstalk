package com.jacamars.dsp.crosstalk.manager;

import java.math.BigDecimal;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.databind.node.MissingNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.tools.ResultSetToJSON;
import com.jacamars.dsp.rtb.bidder.MimeTypes;
import com.jacamars.dsp.rtb.common.Campaign;
import com.jacamars.dsp.rtb.common.Creative;
import com.jacamars.dsp.rtb.common.Deal;
import com.jacamars.dsp.rtb.common.Deals;
import com.jacamars.dsp.rtb.common.Dimension;
import com.jacamars.dsp.rtb.common.Dimensions;
import com.jacamars.dsp.rtb.common.FrequencyCap;
import com.jacamars.dsp.rtb.common.HttpPostGet;
import com.jacamars.dsp.rtb.common.Node;
import com.jacamars.dsp.rtb.db.User;
import com.jacamars.dsp.rtb.exchanges.adx.AdxCreativeExtensions;

/**
 * A class that creates an RTB4FREE creative from the MySQL definition of a creative.
 * @author ben
 *
 */
public class AccountingCreative implements Comparable<Object> {

	/** The campaign this creative belongs to */
	public int campaignid;
	
	/** The id of the cre4ative, as a string */
	public String bannerid; 


	/** Current cost of this creative */
	public volatile AtomicBigDecimal total_cost = new AtomicBigDecimal(0);
	
	/** Total budget of this creative */
	protected volatile AtomicBigDecimal total_budget = new AtomicBigDecimal(0);
	
	/** The bid price of this creative */
	public volatile AtomicBigDecimal bid_ecpm = new AtomicBigDecimal(0);

	/** The epoch time this creative is activated */
	public long activate_time = 0;
	
	/** The epoch time this creative expires */
	public long expire_time = 0;
	
	/** Whether or not this creative was updated in MySQL */
	public long updated = 0;

	/** Width of the creative */
	public int width = 0;
	
	/** Height of the creative */
	public int height = 0;

	/** The image url */
	public String imageurl;
	
	/** The type, as in 'banner' or 'video' */
	String type;
	
	/** This class's logging object */
	static final Logger logger = LoggerFactory.getLogger(AccountingCreative.class);


	// //////////////// BANNER SPECIFIC TARGETING
	
	/** When true, this is a banner, else it is a video. Will need updating for native and audio support */
	protected boolean isBanner;
	
	/** The content type of the banner template */
	protected String contenttype = "";
	
	/** The HTML snippet for the banner */
	public String htmltemplate = "";

	// ////////////// VIDEO SPECIFIC TARGETTING
	
	/** The video duration */
	protected int video_duration = 0;
	
	/** The video width */
	protected int video_width = 0;
	
	/** The video height */
	protected int video_height = 0;
	
	/** The video type */
	protected String video_type = "";
	
	/** The video XML */
	public String video_data = null;
	
	/** The video VAST protcol */
	protected int video_protocol = 2;
	
	/** The video linearity */
	protected int video_linearity = 1;
	
	/** The bitrate of the video */
	protected Integer video_bitrate;
	
	/** The mime type of the video */
	protected String video_mimetype = null;

	/** The table name where this thing is stored in sql */
	protected String tableName = null;

	/** The JSON object that defines this creative from SQL */
	protected ObjectNode myNode;

	/** The RTB4FREE creative that was created from this obhect */
	public Creative creative;

	// If this creative is tagged with categories. Used by bidswitch for example
	private List<String> categories;
	
	/** The RTB4FREE rules nodes that are the constraints for this creative */
	protected List<Node> nodes = new ArrayList<Node>();

	/** The daily budget for the creative */
	volatile AtomicBigDecimal dailyBudget = null;
	
	/** The daily cost incurred today for this creative */
	volatile AtomicBigDecimal dailyCost = null;

	/* The hourly budget for this creative */
	volatile AtomicBigDecimal hourlyBudget = null;
	
	/** The current hourly cost for this creative */
	volatile AtomicBigDecimal hourlyCost = null;

	/** The position on the page for the creative */
	public String position;

	/** SQL name for the total cost attribute */
	protected String TOTAL_COST = "total_cost";
	
	/** SQL name for the hourly cost attribute */
	protected String HOURLY_COST = "hourly_cost";
	
	/** SQL name for the daily cost attribute */
	protected String DAILY_COST = "daily_cost";
	
	/** SQL name for the id of this creative */
	protected String BANNER_ID = "id";
	
	/** SQL name for the campaign that owns this record */
	protected String CAMPAIGN_ID = "campaign_id";
	
	/** SQL name for the image URL attribute */
	protected String IMAGE_URL = "iurl";
	
	/** SQL name for the updated attribute */
	protected String UPDATED = "updated_at";
	
	/** SQL name for the content type attribute */
	protected String CONTENT_TYPE = "contenttype";
	
	/** SQL name for the html snippet for this banner */
	protected String HTML_TEMPLATE = "htmltemplate";
	
	/** SQL name for the daily budget of this creative */
	protected String DAILY_BUDGET = "daily_budget";
	
	/** SQL name for the hourly budget */
	protected String HOURLY_BUDGET = "hourly_budget";
	
	/** SQL name for the vast data attribute */
	protected String VAST_DATA = "vast_video_outgoing_file";

	protected final String INTERSTITIAL = "interstitial";

	protected boolean interstitialOnly = false;

	protected String status = "Active";

	/** The frequency capping specification, as in device.ip */
	private String capSpec;

	/** Number of seconds before the frequency cap expires */
	private int capExpire;

	/** The count limit of the frequency cap */
	private int capCount;

	/** cap time unit **/
	private String capTimeUnit;

	String getType() {
		type = "banner";
		if (!isBanner)
			type = "video";
		return type;
	}

	boolean runUsingElk() {
		type = getType();

		try {
			total_cost.set(Scanner.budgets.getCreativeTotalSpend("" + campaignid, bannerid, type));
			dailyCost.set(Scanner.budgets.getCreativeDailySpend("" + campaignid, bannerid, type));
			hourlyCost.set(Scanner.budgets.getCreativeHourlySpend("" + campaignid, bannerid, type));
		
			logger.debug("*** ELK TEST: Updating budgets: {}/{}/{}",campaignid, bannerid, type);
			logger.debug("Total cost: {} hourly cost: {}, daily_cost: {}",total_cost.getDoubleValue(),
					dailyCost.getDoubleValue(), hourlyCost.getDoubleValue());
		} catch (Exception error) {
			error.printStackTrace();
		}
		return true;
	}
	
	public boolean isActive() throws Exception {

		if (budgetExceeded())
			return false;
		return true;
	}

	public AccountingCreative() {

	}

	public AccountingCreative(ObjectNode myNode, boolean isBanner) throws Exception {
		this.isBanner = isBanner;
		if (isBanner)
			tableName = "banners";
		else
			tableName = "banner_videos";
		update(myNode);
		process();

	}

	public void stop() {

	}

	public void update(ObjectNode myNode) throws Exception {
		this.myNode = myNode;

		double dt = myNode.get(TOTAL_COST).asDouble(0);
		total_cost.set(dt);
		hourlyCost = new AtomicBigDecimal(myNode.get(HOURLY_COST).asDouble(0.0));
		dailyCost = new AtomicBigDecimal(myNode.get(DAILY_COST).asDouble(0.0));

		Object x = myNode.get(DAILY_BUDGET);
		if (x != null && !(x instanceof NullNode)) {
			dailyBudget = new AtomicBigDecimal(myNode.get(DAILY_BUDGET).asDouble());
		}

		x = myNode.get(HOURLY_BUDGET);
		if (x != null && !(x instanceof NullNode)) {
			hourlyBudget = new AtomicBigDecimal(myNode.get(HOURLY_BUDGET).asDouble());
		}

		process();
	}

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	public void process() throws Exception {
		bannerid = myNode.get(BANNER_ID).asText();
		campaignid = myNode.get(CAMPAIGN_ID).asInt();
		if (isBanner) {
			imageurl = myNode.get(IMAGE_URL).asText(null);
		}
		total_budget.set(myNode.get("total_basket_value").asDouble());
		activate_time = myNode.get("interval_start").asLong();
		expire_time = myNode.get("interval_end").asLong();

		updated = myNode.get(UPDATED).asLong(0);

		bid_ecpm.set(myNode.get("bid_ecpm").asDouble());

		String cats = myNode.get("categories").asText("");
		if(!cats.trim().equals("")){
			categories = new ArrayList();
			for (String c : cats.split(",")) {
				categories.add(c.trim());
			}
		}

		if (isBanner) {
			width = myNode.get("width").asInt();
			height = myNode.get("height").asInt();

			contenttype = myNode.get(CONTENT_TYPE).asText();

			htmltemplate = myNode.get(HTML_TEMPLATE).asText();

			htmltemplate = htmltemplate.replaceAll("\n", "");
			htmltemplate = htmltemplate.replaceAll("\r", "");

			if (myNode.get("position") != null)
				position = myNode.get("position").asText(null);
		} else {
			video_duration = myNode.get("vast_video_duration").asInt();
			video_width = myNode.get("vast_video_width").asInt();
			video_height = myNode.get("vast_video_height").asInt();

			video_type = myNode.get("mime_type").asText();
			video_linearity = myNode.get("vast_video_linerarity").asInt();

			video_data = myNode.get(this.VAST_DATA).asText();
			if (myNode.get("vast_video_protocol") != null) {
				video_protocol = myNode.get("vast_video_protocol").asInt();
			}
			video_data = clean(video_data.trim());

			if (myNode.get("bitrate") != null) {
				video_bitrate = new Integer(myNode.get("bitrate").asInt());
			}
		}

		if (myNode.get(INTERSTITIAL) != null && myNode.get(INTERSTITIAL) instanceof MissingNode == false) {
			int x = myNode.get(INTERSTITIAL).asInt();
			if (x == 1)
				interstitialOnly = true;
			else
				interstitialOnly = false;
		} else
			interstitialOnly = false;

		if (myNode.get("status") != null && myNode.get("status") instanceof MissingNode == false) {
			status = myNode.get("status").asText();
		}

		if (myNode.get("frequency_spec") != null) {
			capSpec = myNode.get("frequency_spec").asText(null);
			if (capSpec != null && capSpec.length() != 0) {
				capCount = myNode.get("frequency_count").asInt(0);
				capExpire = myNode.get("frequency_expire").asInt();
				capTimeUnit = myNode.get("frequency_interval_type").asText();
			}
		}
	}

	String clean(String data) {
		String[] lines = data.split("\n");
		String rc = "";
		for (String s : lines) {
			rc += s.trim();
		}
		return rc;
	}

	public boolean changed(ObjectNode node) throws Exception {
		long updated = node.get("updated").asLong();
		if (this.updated != updated)
			return true;
		return false;
	}

	public boolean isBanner() {
		return isBanner;
	}

	public boolean isVideo() {
		if (isBanner)
			return false;
		return true;
	}

	/**
	 * Determine if the budget was exceeded.
	 * @return boolean. Returns true if the budget was exceeded.
	 * @throws Exception on Elk errors.
	 */
	public boolean budgetExceeded() throws Exception {
			logger.debug("********* CHECKING BUDGET FOR CREATIVE {}",bannerid);
			type = getType();
			if (Scanner.budgets == null)
				return false;
			return Scanner.budgets.checkCreativeBudgets(""+campaignid, bannerid, type, total_budget, dailyBudget, hourlyBudget);
	}

	/**
	 * Compile the class into the JSON that will be loaded into Aerospike.
	 * @return Creative. The actual RTB4FREE creative.
	 * @throws Exception on JSON errors.
	 */
	public Creative compile() throws Exception {
		Creative c = new Creative();
		c.imageurl = imageurl;
		nodes.clear();

		c.attributes = nodes;
		c.price = bid_ecpm.doubleValue();
		c.impid = "" + bannerid;
		c.status = this.status;
		c.categories = categories;

		if (isBanner) {
			if (contenttype != null && (contenttype.equalsIgnoreCase("OVERRIDE"))) {
				c.adm_override = true;
			} else {
				c.adm_override = false;
				contenttype = MimeTypes.determineType(htmltemplate);
				if (contenttype != null) {
					Node n = new Node("contenttype", "imp.0.banner.mimes", Node.MEMBER, contenttype);
					n.notPresentOk = true;
					nodes.add(n);
				}
			}

			///////// Handle the height and width ///////////////////////////
			if (width == 0 || height == 0) {
				addDimensions(c);
			} else {
				c.w = width;
				c.h = height;
			}

			c.forwardurl = htmltemplate;

			if (position != null && position.length() > 0) {
				String[] data = position.split(",");
				List<Integer> positions = new ArrayList<Integer>();
				for (String s : data) {
					s = s.trim();
					positions.add(Integer.parseInt(s));
				}
				if (positions.size() > 0) {
					if ((positions.size() == 1 && positions.get(0) != 0) || positions.size() > 0) {
						Node n = new Node("position", "imp.0.banner.pos", Node.INTERSECTS, positions);
						nodes.add(n);
						n.notPresentOk = true;
					}
				}

			}

		} else
			compileVideo(c);

		if (interstitialOnly) {
			Node n = new Node(INTERSTITIAL, "imp.0.instl", Node.EQUALS, 1);
			n.notPresentOk = false;
			nodes.add(n);
		}

		compileAddOns();
		compileExchangeAttributes(c);
		c.attributes = nodes;
		handleDeals(c);

		if (capSpec != null && capSpec.length() > 0 && capCount > 0 && capExpire > 0) {
			c.frequencyCap = new FrequencyCap();
			c.frequencyCap.capSpecification = new ArrayList<String>();
			Targeting.getList(c.frequencyCap.capSpecification, capSpec);
			c.frequencyCap.capTimeout = capExpire;
			c.frequencyCap.capFrequency = capCount;
			c.frequencyCap.capTimeUnit = capTimeUnit;
		}

		doStandardRtb(c);
		return c;
	}

	/**
	 * Attach any defined deals to the creative. deal,deal,deal. Where
	 * deal=id:price, thus id:price,id:price...
	 * 
	 * @param c
	 *            Campaign. The RTB campaign using the dealsl.
	 */
	void handleDeals(Creative c) {
		if (myNode.get("deals") == null)
			return;

		String spec = myNode.get("deals").asText(null);
		if (spec == null || spec.trim().length() == 0)
			return;
		c.deals = new Deals();
		String[] parts = spec.split(",");
		for (String part : parts) {
			Deal d = new Deal();
			String[] subpart = part.split(":");
			d.id = subpart[0].trim();
			d.price = Double.parseDouble(subpart[1].trim());
			c.deals.add(d);
		}
	}

	/**
	 * Compiles the exchange specific attributes, like for Stroer and Adx.
	 * 
	 * @param creative
	 *            Creative. The creative we are attaching the extensions for.
	 */
	public void compileExchangeAttributes(Creative creative) {
		String theKey = "banner_id";

		creative.extensions = new HashMap();
		AdxCreativeExtensions x = null;

		if (!isBanner)
			theKey = "banner_video_id";

		for (JsonNode node : Scanner.exchangeAttributes) {
			String id;
			if ((id = node.get(theKey).asText("-1")).equals(bannerid)) {
				String key = node.get("name").asText(null);
				String value = node.get("value").asText(null);
				String exchange = node.get("exchange").asText("");

				if (exchange.equalsIgnoreCase("adx")) {
					if (x == null) {
						x = new AdxCreativeExtensions();
						creative.adxCreativeExtensions = x;
					}
					switch (key) {
					case "click_thru_url":
						x.adxClickThroughUrl = value;
						break;
					case "tracking_url":
						x.adxTrackingUrl = value;
						break;
					case "vendor_type":
						try {
							x.adxVendorType = new Integer(value);
						} catch (Exception error) {
							x.adxVendorType = 0;
							logger.error("{}/{}, creative  has bad vendor_type: ", bannerid, getType(),value);
						}
						break;
					case "attributes":
						value = value.substring(1, value.length() - 1);
						value = value.replaceAll("\"", "");
						List<String> list = getList(value);
						x.attributes = new ArrayList<Integer>();
						for (String s : list) {
							x.attributes.add(Integer.parseInt(s));
						}
						break;
					}
				} else {
					creative.extensions.put(key, value);
				}
			}
		}
	}

	/**
	 * Compile the video specific components of a creative.
	 * 
	 * @param c
	 *            Campaign. The campaign to attach to.
	 * @throws Exception
	 *             on JSON parsing errors.
	 */
	protected void compileVideo(Creative c) throws Exception {
		c.videoDuration = video_duration;
		c.videoMimeType = video_type;

		///////////// Handle width and height /////////////////////////////
		if (video_width == 0 || video_height == 0) {
			addDimensions(c);
		} else {
			// Old Style
			c.w = video_width;
			c.h = video_height;
		}
		//////////////////////////////////////////////////////////////////

		c.videoProtocol = video_protocol;
		c.attributes = new ArrayList<Node>();

		if (video_bitrate != null) {
			Node n = new Node("contenttype", "imp.0.video.bitrate", Node.GREATER_THAN_EQUALS, video_bitrate);
			n.notPresentOk = true;
			c.attributes.add(n);
		}

		String theVideo;

		c.videoLinearity = video_linearity;
		if (video_data.startsWith("http")) {
			HttpPostGet hp = new HttpPostGet();
			theVideo = hp.sendGet(video_data, 5000, 5000);
		} else if (video_data.startsWith("file")) {
			String fname = video_data.substring(7);
			theVideo = new String(Files.readAllBytes(Paths.get(fname)), StandardCharsets.UTF_8);
		} else {
			theVideo = video_data;
		}

		StringBuilder sb = new StringBuilder(theVideo);
		xmlEscapeEncoded(sb);
		theVideo = sb.toString();

		c.adm = new ArrayList<String>();
		c.adm.add(theVideo);
	}

	void addDimensions(Creative c) {
		String[] parts = null;
		Dimension d = null;
		String key = null;

		// Is this a width dimension?
		if (myNode.get("width_range") != null) {
			key = myNode.get("width_range").asText(null);
			if (key != null) {
				c.dimensions = new Dimensions();
				parts = key.split("-");
				int leftX = Integer.parseInt(parts[0].trim());
				int rightX = Integer.parseInt(parts[1].trim());
				d = new Dimension(leftX, rightX, -1, -1);
				c.dimensions.add(d);
				return;
			}
		}

		// Is this a height dimension
		if (myNode.get("height_range") != null) {
			key = myNode.get("height_range").asText(null);
			if (key != null) {
				c.dimensions = new Dimensions();
				parts = key.split("-");
				int leftY = Integer.parseInt(parts[0].trim());
				int rightY = Integer.parseInt(parts[1].trim());
				d = new Dimension(-1, -1, leftY, rightY);
				c.dimensions.add(d);
				return;
			}
		}

		// Is this WxH, ... list
		if (myNode.get("width_height_list") != null) {
			key = myNode.get("width_height_list").asText(null);
			if (key != null && key.length() > 0) {
				c.dimensions = new Dimensions();
				String[] elements = key.split(",");
				for (String s : elements) {
					parts = s.split("x");
					int w = Integer.parseInt(parts[0].trim());
					int h = Integer.parseInt(parts[1].trim());
					d = new Dimension(w, h);
					c.dimensions.add(d);
				}
			}
		}
	}

	/**
	 * XML Escape a stringbuilder budder.
	 * 
	 * @param sb
	 *            StringBuilder. The data escape.
	 */
	private void xmlEscapeEncoded(StringBuilder sb) {
		int i = 0;
		while (i < sb.length()) {
			i = sb.indexOf("%26", i);
			if (i == -1)
				return;
			if (!(sb.charAt(i + 3) == 'a' && sb.charAt(i + 4) == 'm' && sb.charAt(i + 5) == 'p'
					&& sb.charAt(i + 6) == ';')) {

				sb.insert(i + 3, "amp;");
			}
			i += 7;
		}
	}

	protected void compileAddOns() throws Exception {

	}

	public void addToRTB() throws Exception {
		Configuration config = Configuration.getInstance();
		Campaign selected = null;
		String banid = "" + bannerid;
		String cid = "" + campaignid;

		List<Campaign> campaigns = config.shared.getCampaigns();

		for (int i = 0; i < campaigns.size(); i++) {
			Campaign c = campaigns.get(i);
			if (c.adId.equals(cid)) {
				selected = c;
				for (int j = 0; j < c.creatives.size(); j++) {
					Creative x = c.creatives.get(j);
					if (x.impid.equals(banid)) {
						c.creatives.remove(j);
						break;
					}
				}
			}
		}

		if (selected == null)
			throw new Exception("Failed to find campaign to add: " + campaignid);
		selected.creatives.add(compile());
		config.shared.put(campaigns);
	}

	public void removeFromRTB() throws Exception {
		Configuration config = Configuration.getInstance();

		String banid = "" + bannerid;
		String cid = "" + campaignid;
		List<Campaign> campaigns = config.shared.getCampaigns();
		for (int i = 0; i < campaigns.size(); i++) {
			Campaign c = campaigns.get(i);
			if (c.adId.equals(cid)) {
				for (int j = 0; j < c.creatives.size(); j++) {
					Creative x = c.creatives.get(j);
					if (x.impid.equals(banid)) {
						c.creatives.remove(j);
						config.shared.put(campaigns);
						break;
					}
				}
			}
		}
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		return 0;
	}

	protected List<String> getList(String text) {
		List<String> temp = new ArrayList<String>();
		if (text != null && text.length() > 0) {
			String[] parts = text.split(",");
			for (String part : parts) {
				temp.add(part);
			}
		}
		return temp;

	}

	/**
	 * Call after you compile!
	 * @param creative Creative. The RTB4FREE campaign to send out to the bidders.
	 * @throws Exception
	 *             on JSON errors
	 */
	void doStandardRtb(Creative creative) throws Exception {
		ArrayNode array = ResultSetToJSON.factory.arrayNode();
		ArrayNode list;
		String rkey;
		int theId = Integer.parseInt(bannerid);
		if (isBanner) {
			list = Scanner.bannerRtbStd;
			rkey = "banner_id";
		} else {
			list = Scanner.videoRtbStd;
			rkey = "banner_video_id";

		}
		for (int i = 0; i < list.size(); i++) {
			JsonNode node = list.get(i);
			if (theId == node.get(rkey).asInt()) {
				Integer key = node.get("rtb_standard_id").asInt();
				JsonNode x = Scanner.gloablRtbSpecification.get(key);
				array.add(x);
			}
		}
		RtbStandard.processStandard(array, creative.attributes);
	}


	public void checkBudgetUpdates() throws Exception {
		String table = "banners";
		if (isVideo())
			table = "banner_videos";

		String select = "select * from " + table + " where id=" + bannerid + " and  campaign_id = " + campaignid;
		Statement stmt = Configuration.getInstance().getConnection().createStatement();
		ResultSet rs = stmt.executeQuery(select);
		rs.next();
		BigDecimal stage;
		stage = rs.getBigDecimal("total_cost");
		if (stage == null)
			stage = new BigDecimal(0);
		total_cost = new AtomicBigDecimal(stage);

		stage = rs.getBigDecimal("daily_cost");
		if (stage == null)
			stage = new BigDecimal(0);
		dailyCost = new AtomicBigDecimal(stage);

		stage = rs.getBigDecimal("hourly_cost");
		if (stage == null)
			stage = new BigDecimal(0);
		hourlyCost = new AtomicBigDecimal(stage);
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
}
