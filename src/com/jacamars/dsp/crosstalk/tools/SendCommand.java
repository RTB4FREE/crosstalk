package com.jacamars.dsp.crosstalk.tools;

import com.jacamars.dsp.rtb.common.HttpPostGet;
import com.jacamars.dsp.crosstalk.api.DeleteCmd;
import com.jacamars.dsp.crosstalk.api.GetBudgetCmd;
import com.jacamars.dsp.crosstalk.api.GetJsonCmd;
import com.jacamars.dsp.crosstalk.api.GetPriceCmd;
import com.jacamars.dsp.crosstalk.api.GetStatusCmd;
import com.jacamars.dsp.crosstalk.api.GetValuesCmd;
import com.jacamars.dsp.crosstalk.api.PingCmd;
import com.jacamars.dsp.crosstalk.api.RefreshCmd;
import com.jacamars.dsp.crosstalk.api.SetBudgetCmd;
import com.jacamars.dsp.crosstalk.api.SetPriceCmd;
import com.jacamars.dsp.crosstalk.api.StartBidderCmd;
import com.jacamars.dsp.crosstalk.api.StopBidderCmd;
import com.jacamars.dsp.crosstalk.api.UpdateCmd;

public class SendCommand {

	static String url = "http://localhost:8100/api";
	static String username = "ben";
	static String password = "test";
	static String campaign = null;
	static String creative = null;
	static String bidder = null;
	static String cmd = "ping";
	static Double total = null;
	static Double hourly = null;
	static Double daily = null;
	static double price;
	//static String url = "http://54.147.50.159:8100/api";
	static HttpPostGet hp = new HttpPostGet();
	public static void main(String args[]) throws Exception {
		//ping();
		//getStatus();
		//startBidder();
		//stopBidder();
		//sendRefresh();
		//getBudget();
		//setBudget();
		//getValues();
		//setValues();
		//sendRefresh();
		//sendUpdate();
		
		int i = 0;
		while(i<args.length) {
			switch(args[i]) {
			case "-host": 
				String host = args[i+1];
				if (host.contains(":")) 
					url = "http://" + host + "/api";
				else
					url = "http://" + host + ":8100/api";
				i+=2;
				break;
			case "-total":
				total = new Double(Double.parseDouble(args[+1]));
				i+=2;
				break;
			case "-daily":
				daily = new Double(Double.parseDouble(args[+1]));
				i+=2;
				break;
			case "-hourly":
				hourly = new Double(Double.parseDouble(args[+1]));
				i+=2;
				break;
			case "-b":
				bidder = args[i+1];
				i+=2;
				break;
			case "-u":
				username = args[i+1];
				i+=2;
				break;
			case "-p":
				password = args[i+1];
				i+=2;
				break;
			case "-adid": 
				campaign = args[i+1];
				i+=2;
				break;
			case "-crid":
				creative = args[i+1];
				i+=2;
				break;
			case "-c":
				cmd = args[i+1];
				i+=2;
				break;
			case "-price":
				price = Double.parseDouble(args[i+1]);
				i+=2;
				break;
			case "-help":
			case "-h":
				System.out.println("-host   hostname[:port]");
				System.out.println("-total  floating-point-number");
				System.out.println("-daily  floating-point-number");
				System.out.println("-hourly floating-point-number");
				System.out.println("-b      bidder-name");
				System.out.println("-u      username");
				System.out.println("-p      password");
				System.out.println("-price  double-value");
				System.out.println("-adid   campaign-ad-id");
				System.out.println("-crid   creative-id");
				System.out.println("-cmd    name");
				System.out.println("        One of: ping delete getbudget getjson getprice getstatus getvalues");
				System.out.println("              : refresh setbudget setprice setvalues startbidder stopbidder update");
				return;
			default:
				System.out.println("Unknown: " + args[i]);
				return;
			}		
		}
		
		switch(cmd) {
		case "ping":
			ping();
			break;
		case "getjson":
			getJson();
			break;
		case "getstatus":
			getStatus();
			break;
		case "startbidder":
			startBidder();
			break;
		case "stopbidder":
			stopBidder();
			break;
		case "update":
			sendUpdate();
			break;
		case "refresh":
			sendRefresh();
			break;
		case "getbudget":
			getBudget();
			break;
		case "setbudget":
			setBudget();
			break;
		case "getvalues":
			getValues();
			break;
		case "getprice":
			getPrice();
			break;
		case "delete":
			sendDelete();
		}
	}
	
	public static void getJson() throws Exception {
		GetJsonCmd cmd = new GetJsonCmd(username,password,campaign);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void ping() throws Exception {
		PingCmd ping = new PingCmd(username,password);
		String content = ping.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void getStatus() throws Exception {
		GetStatusCmd cmd = new GetStatusCmd(username,password,bidder);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);;
	}
	
	public static void sendDelete() throws Exception {
		DeleteCmd cmd = new DeleteCmd(username,password,campaign);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);;
	}
	
	public static void startBidder() throws Exception {
		StartBidderCmd cmd = new StartBidderCmd(username,password,bidder);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);;
	}
	
	public static void stopBidder() throws Exception {
		StopBidderCmd cmd = new StopBidderCmd(username,password,bidder);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);;
	}
	
	public static void sendUpdate() throws Exception {
		UpdateCmd cmd = new UpdateCmd(username,password,campaign);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content,300000,300000);
		System.out.println(returns);;
	}
	
	public static void sendRefresh() throws Exception {
		RefreshCmd cmd = new RefreshCmd(username,password);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content,300000,300000);
		System.out.println(returns);;
	}
	
	public static void getBudget() throws Exception {
		GetBudgetCmd cmd = new GetBudgetCmd(username,password,campaign,creative);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void getPrice() throws Exception {
		GetPriceCmd cmd = new GetPriceCmd(username,password,campaign,creative);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void setPrice() throws Exception {
		SetPriceCmd cmd = new SetPriceCmd(username,password,campaign,creative, price);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void setBudget() throws Exception {
		SetBudgetCmd cmd = new SetBudgetCmd(username,password,campaign,creative,hourly,daily,total);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
	
	public static void getValues() throws Exception {
		GetValuesCmd cmd = new GetValuesCmd(username,password,campaign,creative);
		String content = cmd.toJson();
		System.out.println(content);
		String returns = hp.sendPost(url, content);
		System.out.println(returns);
	}
}
