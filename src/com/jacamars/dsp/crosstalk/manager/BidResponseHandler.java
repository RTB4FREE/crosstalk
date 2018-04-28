package com.jacamars.dsp.crosstalk.manager;


import com.fasterxml.jackson.core.JsonProcessingException;

import com.jacamars.dsp.rtb.pojo.BidResponse;
import java.util.concurrent.atomic.LongAdder;

/**
 * A publisher for ZeroMQ based messages for bids, sharable by multiple threads.
 * 
 * @author Ben M. Faul
 *
 */
public class BidResponseHandler extends WinHandler {

	public static volatile LongAdder adder = new LongAdder();
	/**
	 * Constructor for base class.
	 * @param name String. The name of the log
	 * @throws Exception  on File errors
	 */
	public BidResponseHandler(String name) throws Exception {
		super(name);
	}

	public void add(BidResponse e) {
		synchronized(sb) {
			String contents;
			try {
				if (e.timestamp == 0)
					e.timestamp = System.currentTimeMillis();
				contents = mapper.writer().writeValueAsString(e);
				sb.append(contents);
				adder.increment();
				sb.append("\n");
			} catch (JsonProcessingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	/**
	 * Return the bid count and Reset
	 * @return
	 */
	public static long getCountAndReset() {
		long value = adder.longValue();
		adder.reset();
		return value;
	}
}