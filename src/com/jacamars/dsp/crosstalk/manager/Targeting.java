package com.jacamars.dsp.crosstalk.manager;

import java.util.*;

import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.tools.IABCategories;
import com.jacamars.dsp.rtb.common.Node;


/**
 * The target object used by the AccountingCampaign object/.t
 * @author Ben M. Faul
 *
 */
public class Targeting {

	public static final String NONE = "NONE";
	public static final String BLACKLIST = "BLACKLIST";
	public static final String WHITELIST = "WHITELIST";

	public int campaignid;
	public int targetingid;

	public String domainstring = "";
	
	/**
	 * Its either a listofdomains or a symbol that is al ist.
	 */
	public List<String> listofdomains = new ArrayList<String>();
	public String listofdomainsSYMBOL = null;
			
	public String domaintargetting = "";

	public String carrier = "";
	public String connectionType = "";

	public String user_agent;
	public String os = "";
	public String make = "";
	public String model = "";
	public String os_version = "";
	public String browser = "";
	public String country = "";


	public double geo_latitude = 0;
	public double geo_longitude = 0;
	public double geo_range = 0;

	public List<Node> nodes = new ArrayList<Node>();

	public String IAB_category = "";
	public String IAB_category_blklst = "";
	public String carriers = "";

	List<String> listofpages = new ArrayList<String>();
	List<String> transparency = new ArrayList<String>();
	List<String> devicetypes = new ArrayList<String>();
	List<Integer> rtbStandardIds = new ArrayList<Integer>();
	List<Integer> connectionTypes = new ArrayList<>();

	protected ObjectNode myNode;

	protected String LIST_OF_DOMAINS = "list_of_domains";
	protected String DOMAIN_TARGETTING = "domain_targetting";

	protected String LIST_OF_PAGES = "listofpages";
	protected String TRANSPARENCY = "pagetransparency";
	protected String RTB_STANDARD = "rtb_standard";

	protected AccountingCampaign parent;

	public Targeting() {

	}

	public Targeting(AccountingCampaign parent, ObjectNode myNode)
			throws Exception {
		this.myNode = myNode;
		this.parent = parent;
		process();
	}

	// ///////////////////////////////////////////////////////////////////////////////////////////////////////
	public void process() throws Exception {
		if (myNode.get(LIST_OF_DOMAINS) != null) {
			String str = myNode.get(LIST_OF_DOMAINS).asText(null);
			if (str != null && str.trim().length()>0) {
				
				if (str.startsWith("$")) {
					listofdomainsSYMBOL = str;
				} else
					getList(listofdomains, str);
			}
		}

		if (myNode.get(DOMAIN_TARGETTING) != null)
			domaintargetting = myNode.get(DOMAIN_TARGETTING).asText();
		carrier = myNode.get("carrier").asText();

		country = myNode.get("country").asText("");
		
		os = myNode.get("os").asText("");

		if (myNode.get(LIST_OF_PAGES) != null) {
			String test = myNode.get(LIST_OF_PAGES).asText(null);
			if (test != null) {
				if (!test.equals("X") && test.length() != 0) {
					getList(listofpages, test);
					test = myNode.get(TRANSPARENCY).asText(null);
					if (test != null)
						getList(transparency, test);
				}
			}
		}
		
		if (myNode.get("devicetype") != null) {
			String test = myNode.get("devicetype").asText(null);
			if (test != null && test.length() != 0) {
				getList(devicetypes, test);
			}
		}

		if (myNode.get("make") != null)
			make = myNode.get("make").asText();
		if (myNode.get("model") != null)
			model = myNode.get("model").asText();
		geo_latitude = myNode.get("geo_latitude").asDouble();
		geo_longitude = myNode.get("geo_longitude").asDouble();

		// connectionTYpe is needed here

		// td = Test.getValue(geo_latitude,"geo_range",rs);
		// geo_range = td.value; delta |= ts.delta;

		if (myNode.get("IAB_category") != null)
			IAB_category = myNode.get("IAB_category").asText();
		if (myNode.get("IAB_category_blklist") != null)
			IAB_category_blklst = myNode.get("IAB_category_blklist").asText();
		
		
		if (myNode.get(RTB_STANDARD) != null) {
			String rtbs =  myNode.get(RTB_STANDARD).asText();
			getIntegerList(rtbStandardIds, rtbs);
		}

		if (myNode.get("connectiontype") != null && myNode.get("connectiontype") instanceof MissingNode == false) {
			String connections = myNode.get("connectiontype").asText();
			if (connections.equals("null")==false && connections.length()>0)
				getIntegerList(connectionTypes,connections);
		} else {
			connectionTypes.clear();
		}
		
		nodes = compile();
	}

	// /////////////////////////////////////////////////////////////////////////////////////////

	public static boolean getList(List<String> list, String text) {
		boolean delta = false;

		text = text.trim();
		if (text.contains(",")) {
			if (text.startsWith("["))
				text = text.substring(1);
			if (text.endsWith("]"))
				text = text.substring(0,text.length()-1);
		}
		List<String> temp = new ArrayList<String>();
		temp.addAll(list);
		
		/**
		 * Ok, I presume the user doesn't add dups, though the bidder doesn't care it will turn it into a Set anyway.
		 */
		if (text != null) {
			String[] parts = text.split(",");
			for (String part : parts) {
				part = part.trim();
				part = part.replaceAll("\t","");
				part = part.replaceAll("\n", "");
				part = part.replaceAll("\r", "");
				list.add(part);
			}
		}

		delta |= list.size() != temp.size();
		return delta;
	}
	
	public static boolean getIntegerList(List<Integer> list, String text) {
		boolean delta = false;

		text = text.trim();
		if (text.contains(",")) {
			if (text.startsWith("["))
				text = text.substring(1);
			if (text.endsWith("]"))
				text = text.substring(0,text.length()-1);
		}
		
		if (text != null) {
			String[] parts = text.split(",");
			for (String part : parts) {
				part = part.trim();
				part = part.replaceAll("\t","");
				part = part.replaceAll("\n", "");
				part = part.replaceAll("\r", "");
				Integer test = Integer.parseInt(part);
				if (!list.contains(test)) {
					delta = true;
					list.add(test);
				}
			}
		}

		return delta;
	}

	protected List<Node> compile() throws Exception {
		nodes = new ArrayList<Node>();
		Node n = null;

		//
		// Domain blacklist/whitelist Can be an actual list or a symbol
		//
		if (listofdomains != null && listofdomains.size() != 0) {
			boolean bl = true;
			if ((bl=domaintargetting.equalsIgnoreCase("BLACKLIST"))==true)
				n = new Node("blacklist", "domain", Node.NOT_MEMBER,
						listofdomains);
			else if (domaintargetting.equalsIgnoreCase("WHITELIST"))
				n = new Node("whitelist", "domain", Node.MEMBER,
						listofdomains);

			if (n != null) {
				if (bl)
					n.notPresentOk = true;
				else
					n.notPresentOk = false;
				nodes.add(n);

				if (listofpages != null && listofpages.size() != 0) {
					boolean notPresentOk = false;
					int op = Node.STRINGIN;
					if (transparency.contains("N"))
						op = Node.NOT_STRINGIN;
					if (transparency.contains("NOTREQUIRED"))
						notPresentOk = true;

					n = new Node("pagetest", "site.page", op, listofpages);
					n.notPresentOk = notPresentOk;
					nodes.add(n);
				}
			}
		} else {
			if (listofdomainsSYMBOL != null) {
				Node n1 = null;
				boolean bl = true;
				if ((bl=domaintargetting.equalsIgnoreCase("BLACKLIST"))==true) {
					n = new Node("blacklist", "domain", Node.NOT_MEMBER,
							listofdomainsSYMBOL);
					//n1 = new Node("blacklist", "app.domain", Node.NOT_MEMBER,
					//		listofdomainsSYMBOL);
				}
				else if (domaintargetting.equalsIgnoreCase("WHITELIST")) {
					n = new Node("whitelist", "domain", Node.MEMBER,
							listofdomainsSYMBOL);
					//n1 = new Node("whitelist", "app.domain", Node.MEMBER,
					//		listofdomainsSYMBOL);
					
				}

				if (bl)
					n.notPresentOk = true;
				else
					n.notPresentOk = false;
				//n1.notPresentOk = true;

				nodes.add(n);
				//nodes.add(n1);
			}
		}
		
		if (devicetypes != null && devicetypes.size() != 0) {
			List<Integer> list = new ArrayList<Integer>();
			for (String s : devicetypes) {
				if (!s.equals("0")) {
					list.add(Integer.parseInt(s));
				}
			}
			if (list.size() > 0) {
				n = new Node("devicetypes", "device.devicetype", Node.INTERSECTS, list);
				n.notPresentOk = false;
				nodes.add(n);
			}
		}

		if (carrier.length() > 0 && !carrier.equals("null")) {
			String[] parts = carrier.split(",");
			n = new Node("carrier", "device.carrier", Node.MEMBER, parts);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (os.length() > 0 && !os.equals("null")) {
			List<String> list = new ArrayList<String>();
			getList(list, os);
			n = new Node("os", "device.os", Node.MEMBER, list);
			n.notPresentOk = false;
			nodes.add(n);

			if (os_version.length() > 0 && !os_version.equals("null")) {
				list = new ArrayList<String>();
				getList(list, os_version);
				n = new Node("os_version", "device.os.version", Node.MEMBER,
						list);
				n.notPresentOk = false;
				nodes.add(n);
			}
		}

		if (browser.length() > 0 & !browser.equals("null")) {
			List<String> list = new ArrayList<String>();
			getList(list, browser);
			n = new Node("browser", "device.browser", Node.MEMBER, list);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (country.length() > 0 && !country.equals("null")) {
			List<String> countries = setCountries(country);
			n = new Node("country", "device.geo.country", Node.MEMBER,
					countries);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (carriers.length() > 0 && !carriers.equals("null")) {
			String[] parts = carriers.split(",");
			n = new Node("carriers", "device.carrier", Node.MEMBER, parts);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (connectionType.length() > 0
				&& !connectionType.equals("null")) {
			String[] parts = connectionType.split(",");
			List<Integer> list = new ArrayList<Integer>();
			for (String part : parts) {
				list.add(Integer.parseInt(part));
			}
			n = new Node("", "device.connectiontype", Node.MEMBER, list);
			n.notPresentOk = false;
			nodes.add(n);
		}

		/////////////////////////////////////
		//
		// IAB Categories
		//
		if (IAB_category.length() > 0 && !IAB_category.equals("null")) {
			String[] parts = IAB_category.split(",");
			for (int i = 0; i < parts.length; i++) {
				String s = parts[i];
				s = s.replaceAll("\"", "");
				parts[i] = s;
			}
			n = new Node("matching-categories", "site.cat", Node.INTERSECTS,
					parts);
			n.notPresentOk = false;
			Node n1 = new Node("matching-categories", "app.cat", Node.INTERSECTS,
					parts);
			n1.notPresentOk = false;
			List<Node> orList = new ArrayList<Node>();
			orList.add(n);
			orList.add(n1);

			Node ornode = new Node("ortest", null, Node.OR,orList);
			nodes.add(ornode);
		}
		if (IAB_category_blklst.length() > 0 && !IAB_category_blklst.equals("null")) {
			String[] parts = IAB_category_blklst.split(",");
			for (int i = 0; i < parts.length; i++) {
				String s = parts[i];
				s = s.replaceAll("\"", "");
				parts[i] = s;
			}
			n = new Node("nonmatching-categories", "site.cat", Node.NOT_INTERSECTS,
					parts);
			n.notPresentOk = true;
			Node n1 = new Node("nonmatching-categories", "app.cat", Node.NOT_INTERSECTS,
					parts);
			n1.notPresentOk = true;
			List<Node> orList = new ArrayList<Node>(Arrays.asList(n, n1));;

			Node ornode = new Node("ortest", null, Node.OR,orList);
			nodes.add(ornode);
		}
		////////////////////////////////////////////////////////////////////////////////////////////////////////

		if (make.length() > 0 && !make.equals("null")) {
			String[] parts = make.split(",");
			n = new Node("make", "device.make", Node.MEMBER, parts);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (model.length() > 0 && !model.equals("null")) {
			String[] parts = model.split(",");
			n = new Node("model", "device.model", Node.MEMBER, parts);
			n.notPresentOk = false;
			nodes.add(n);
		}

		if (geo_latitude != 0.0 && geo_longitude != 0 && geo_range != 0) {
			Map<String, Double> m = new HashMap<String, Double>();
			m.put("lat", geo_latitude);
			m.put("lon", geo_longitude);
			m.put("range", geo_range);
			List<Map<String, Double>> list = new ArrayList<Map<String, Double>>();
			list.add(m);
			n = new Node("LATLON", "device.geo", Node.INRANGE, list);
			nodes.add(n);
		}

		/**
		 * Connection types
		 */
		if (connectionTypes.size() > 0) {
			n = new Node("connectiontypes", "device.connectiontype", Node.MEMBER, connectionTypes);
			n.notPresentOk = false;
			nodes.add(n);
		}

		return nodes;

	}

	/**
	 * Set the country, but fix US to USA. Strip off garbage characters like ' and "
	 * @param test String. The list of string comma separated.
	 * @return List. Returns a list of countries.
	 * @throws Exception on regex errors.
	 */
	public List<String> setCountries(String test) throws Exception {
		List<String> countries = new ArrayList<String>();
		if (test.length() > 0 && !test.equals("null")) {
			String parts[] = test.split(",");
			for (int i = 0; i < parts.length; i++) {
				String value = parts[i];
				value = value.replaceAll("\"", "");
				if (value.equals("US"))
					value = "USA";
				countries.add(value);
			}
		}
		return countries;
	}
}
