package com.jacamars.dsp.crosstalk.tools;

import com.jacamars.dsp.rtb.common.HttpPostGet;
import com.jacamars.dsp.crosstalk.api.GetBudgetCmd;
import com.jacamars.dsp.crosstalk.api.GetPriceCmd;
import com.jacamars.dsp.crosstalk.api.GetStatusCmd;
import com.jacamars.dsp.crosstalk.api.PingCmd;
import com.jacamars.dsp.crosstalk.api.SetPriceCmd;

// api -h host -p 8100 -c ping
//
public class Api {

	static String host = "localhost";
	static int port = 8100;
	static String username = "ben";
	static String password = "xyz";
	static HttpPostGet hp = new HttpPostGet();
	static String url;
	
	public static void main(String [] args) throws Exception {

		url = "http://"+host+":"+port+"/api";
		//doPing();
		//doGetStatus();
		//doGetPrice();
		//doSetPrice();
		doGetBudget();
	}
	
	public static void doPing() throws Exception {
		PingCmd ping = new PingCmd(username,password);
		
		String str = ping.toJson();
		System.out.println("--->" + str);
		
		str = hp.sendPost(url,str);
		if (hp.getResponseCode() != 200)
			throw new Exception("API returned unexpected code: " + hp.getResponseCode());
		System.out.println(str);
	}
	
	public static void doGetStatus() throws Exception {
		GetStatusCmd cmd = new GetStatusCmd(username,password,"carbon-x1:8080");
		
		String str = cmd.toJson();
		System.out.println("--->" + str);
		
		str = hp.sendPost(url,str);
		if (hp.getResponseCode() != 200)
			throw new Exception("API returned unexpected code: " + hp.getResponseCode());
		System.out.println(str);
	}
	
	public static void doGetPrice() throws Exception {
		GetPriceCmd cmd = new GetPriceCmd(username,password,"7","9");
		
		String str = cmd.toJson();
		System.out.println("--->" + str);
		
		str = hp.sendPost(url,str);
		if (hp.getResponseCode() != 200)
			throw new Exception("API returned unexpected code: " + hp.getResponseCode());
		System.out.println(str);
	}
	

	public static void doSetPrice() throws Exception {
		SetPriceCmd cmd = new SetPriceCmd(username,password,"7","9",.223);
		
		String str = cmd.toJson();
		System.out.println("--->" + str);
		
		str = hp.sendPost(url,str);
		if (hp.getResponseCode() != 200)
			throw new Exception("API returned unexpected code: " + hp.getResponseCode());
		System.out.println(str);
	}
	
	public static void doGetBudget() throws Exception {
		GetBudgetCmd cmd = new GetBudgetCmd(username,password,"7", "9");
		
		String str = cmd.toJson();
		System.out.println("--->" + str);
		
		str = hp.sendPost(url,str);
		if (hp.getResponseCode() != 200)
			throw new Exception("API returned unexpected code: " + hp.getResponseCode());
		System.out.println(str);
	}
}
