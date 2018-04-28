package com.jacamars.dsp.crosstalk.tools;


import java.util.List;

import com.jacamars.dsp.rtb.redisson.RedissonClient;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.jacamars.dsp.rtb.db.DataBaseObject;

public class ShowRedis {
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
		DataBaseObject shared = DataBaseObject.getInstance(redis);

		List list = shared.getCampaigns();
		String str = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(list);
		System.out.println(list);
	}
}
