package com.jacamars.dsp.crosstalk.tools;


import java.util.List;
import java.util.Map;

import com.jacamars.dsp.rtb.redisson.RedissonClient;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.jacamars.dsp.rtb.tools.NameNode;

public class GetStatus {
	static InitStub stub = new InitStub();
	public static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	
	public static void main(String [] args) throws Exception {
		
		String pub = "tcp://localhost:2001";
		String sub = "tcp://localhost:2000";
		String user = null;
		int i = 0;
		while(i<args.length) {
			switch(args[i]) {
			case "-h":
				System.out.println("-p tcp://localhost:2000   [Xpubsub publisher        ]");
				System.out.println("-s tcp://localhost:2001   [XPubsub subscriber       ]");
				System.exit(1);
			case "-p":
				pub = args[i+1];
				i+=2;
				break;
			case "-s":
				sub = args[i+1];
				i+=2;
				break;
			default:
				System.err.println("Huh?");
				System.exit(1);
			}
		}

		RedissonClient redis = new RedissonClient();
		redis.setSharedObject(pub,sub);

		double now = System.currentTimeMillis() + 100000;
		List<String> members = getBidders(redis);
		
		for (String s : members) {
			Map m = (Map)redis.hgetAll(s);
			if (m != null)
				System.out.println(mapper.writer().withDefaultPrettyPrinter().writeValueAsString(m));
			else
				System.out.println(s + "=" + m);
		}
	}
	
	public static List<String>getBidders(RedissonClient r) throws Exception {
		List<String> members = r.getList(NameNode.BIDDERSPOOL);
		return members;
	}
}
