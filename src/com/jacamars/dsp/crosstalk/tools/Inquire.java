package com.jacamars.dsp.crosstalk.tools;

import java.sql.PreparedStatement;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.AccountingCreative;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.Scanner;

public class Inquire {

	Configuration config;

	public static void main(String... args) throws Exception {
		List<String> list = new ArrayList();
		String file = "config.json";

		if (args.length > 0) {
			if (args[0].equals("-h") || args[0].equals("-help")) {
				System.out.println("-h            This message.");
				System.out.println("[ filename ]  The name of the config file to use, default is config.json");
				System.exit(0);
			}
			file = args[0];
			for (int i = 1; i < args.length; i++) {
				list.add(args[i]);
			}
		}

		new Inquire(file, list);
	}

	public Inquire(String fileName, List<String> list) throws Exception {
		config = Configuration.getInstance();
		config.initialize(fileName, null, null); // initialize the file, but
													// don't add the hooks yet

		config.initialize(fileName, null, null);

		// scanner = new Scanner(config);

		config.connectToSql();

		Configuration config = Configuration.getInstance();
		String select = "select * from campaigns order by id DESC";
		if (list != null && list.size() > 0) {
			String values = "(";
			for (String s : list) {
				values += s + ",";
			}
			values = values.substring(0, values.length() - 1);
			values += ")";
			select = "select * from campaigns where id in " + values + " order by id DESC";
		}

		PreparedStatement prep = config.getConnection().prepareStatement(select);
		ResultSet rs = prep.executeQuery();

		ArrayNode array = ResultSetToJSON.convert(rs);

		Scanner.handleNodes(array);

		////////////////////////////////////////////////
		String status = "NA";

		for (int i = 0; i < array.size(); i++) {
			ObjectNode node = (ObjectNode) array.get(i);
			AccountingCampaign camp;
			try {
				camp = new AccountingCampaign(node);
				boolean active = camp.isActive();
				boolean budget = camp.budgetExceeded();
				boolean expired = camp.isExpired();
				if (node.get("status") != null)
					status = node.get("status").asText();
				System.out.println(String.format("\n%d\tstatus=%s\tactive=%b\tover budget=%b\texpired=%b",
						camp.campaignid, status, active, budget, expired));

				System.out.println("\n\tImp\tType\tActive\tExpired\t>Budget\tWidth\tHeight\tMarkup");
				ArrayNode xnode = (ArrayNode) node.get("banner");
				if (xnode != null) {
					for (int k = 0; k < xnode.size(); k++) {
						ObjectNode znode = (ObjectNode) xnode.get(k);
						AccountingCreative creative = new AccountingCreative(znode, true,false);

						boolean isActive = creative.isActive();
						boolean isExceeded = creative.budgetExceeded();
						int width = creative.width;
						int height = creative.height;
						String markup = creative.htmltemplate;
						if (markup.length() > 64) {
							markup = markup.substring(0, 64);
							markup += "...";
						}
						System.out.println(String.format("\t%s\tBANNER\t%b\t%b\t%d\t%d\t%s", creative.bannerid,
								isActive, isExceeded, width, height, markup));
					}
				}

				xnode = (ArrayNode) node.get("banner_video");
				if (xnode != null) {
					for (int k = 0; k < xnode.size(); k++) {
						ObjectNode znode = (ObjectNode) xnode.get(k);
						AccountingCreative creative = new AccountingCreative(znode, false,false);

						boolean isActive = creative.isActive();
						boolean isExceeded = creative.budgetExceeded();
						int width = creative.width;
						int height = creative.height;
						String markup = creative.video_data;
						if (markup.length() > 64) {
							markup = markup.substring(0, 64);
							markup += "...";
						}
						System.out.println(String.format("\t%s\tVIDEO\t%b\t%b\t%d\t%d\t%s", creative.bannerid,
								isActive, isExceeded, width, height, markup));
					}
				}
				camp.compile();
				if ((list != null) && (list.size() > 0)) {
					System.out.println(camp.getOutput());
				}
				
				xnode = (ArrayNode) node.get("banner_audio");
				if (xnode != null) {
					for (int k = 0; k < xnode.size(); k++) {
						ObjectNode znode = (ObjectNode) xnode.get(k);
						AccountingCreative creative = new AccountingCreative(znode, false,true);

						boolean isActive = creative.isActive();
						boolean isExceeded = creative.budgetExceeded();
						int width = creative.width;
						int height = creative.height;
						String markup = creative.video_data;
						if (markup.length() > 64) {
							markup = markup.substring(0, 64);
							markup += "...";
						}
						System.out.println(String.format("\t%s\tAUDIO\t%b\t%b\t%d\t%d\t%s", creative.bannerid,
								isActive, isExceeded, width, height, markup));
					}
				}
				camp.compile();
				if ((list != null) && (list.size() > 0)) {
					System.out.println(camp.getOutput());
				}
				System.out.println("========================================================================================================");
			} catch (Exception error) {
				error.printStackTrace();
				System.out.println(String.format("\t*** ERROR **** %s", error.toString()));
			}
		}

		System.exit(0);
	}

	public String getId(ObjectNode node) {
		return "" + node.get("campaignid").asInt();
	}
}
