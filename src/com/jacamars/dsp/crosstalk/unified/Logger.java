package com.jacamars.dsp.crosstalk.unified;

import java.io.BufferedReader;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonInclude.Include;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.rtb.bidder.ZPublisher;
import com.jacamars.dsp.rtb.pojo.BidResponse;
import com.jacamars.dsp.rtb.pojo.WinObject;

public class Logger {
	private final DelayQueue<ExpiringKey> delayQueue = new DelayQueue<ExpiringKey>();
	private final Map<String, ExpiringKey> map = new ConcurrentHashMap<String,ExpiringKey>();
	private final ZPublisher publisher;
	protected static String directory;

	public Logger(String directory, ZPublisher publisher) {
		Logger.directory = directory;
		this.publisher = publisher;

		Arrays.stream(new File(directory).listFiles()).forEach(File::delete);

		ScheduledExecutorService execService = Executors.newScheduledThreadPool(5);
		execService.scheduleAtFixedRate(() -> {
			try {
				cleanup();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 1000L, 1000L, TimeUnit.MILLISECONDS);
	}

	/**
	 * When a key expires it can be pulled from the delayqueue, then you can
	 * remove it, and what it references.
	 */
	private void cleanup() {
		ExpiringKey delayedKey = delayQueue.poll();
		while (delayedKey != null) {
			try {

				map.remove(delayedKey.getKey());
				publisher.addString(delayedKey.get());
				File f = new File(directory + "/" + delayedKey.getKey());
				f.delete();

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void testComplete(ExpiringKey key) {
		if (key.complete()) {
			key.expire();
		}
	}

	public void addRequest(String oid, Map request) throws Exception {
		ExpiringKey key = map.get(oid);
		if (key == null) {
			key = new ExpiringKey(oid, request);
		} else
			key.setRequest(request);
	}

	public void addWin(String oid, WinObject win) throws Exception {
		ExpiringKey key = map.get(oid);
		if (key == null) {
			key = new ExpiringKey(oid, win);
		} else
			key.setWin(win);
		;
	}

	public void addBid(String oid, BidResponse response) throws Exception {
		ExpiringKey key = map.get(oid);
		if (key == null) {
			key = new ExpiringKey(oid, response);
		} else
			key.setBid(response);
	}

	/**
	 * The expiring key. Implements Delayed so it can be used to trigger map
	 * deletions.
	 * 
	 * @author Ben M. Faul
	 *
	 */
	private class ExpiringKey implements Delayed {

		// The time of creation.
		private long startTime = System.currentTimeMillis();
		// The max time of life
		private final long maxLifeTimeMillis;
		// The key
		private final String key;

		private boolean bid;
		private boolean win;
		private boolean request;

		ObjectMapper mapper = new ObjectMapper();

		/**
		 * Create an expiring key.
		 * 
		 * @param key
		 *            K. The key name.
		 * @param request
		 * 			  Map. The request to store.
		 * @throws Exception on errors setting the request.
		 */
		public ExpiringKey(String key, Map request) throws Exception {
			this.maxLifeTimeMillis = 300000;
			this.key = key;
			setRequest(map);
		}

		public ExpiringKey(String key, BidResponse response) throws Exception {
			this.maxLifeTimeMillis = 300000;
			this.key = key;
			setBid(response);
		}

		public ExpiringKey(String key, WinObject win) throws Exception {
			this.maxLifeTimeMillis = 300000;
			this.key = key;
			setWin(win);
			this.win = true;
		}

		private void output(String x, Object obj) throws Exception {
			mapper.setSerializationInclusion(Include.NON_NULL);
			mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			String content = mapper.writer().writeValueAsString(obj);
			StringBuilder sb = new StringBuilder();
			if (bid || win || request)
				sb.append(",");
			sb.append("\"");
			sb.append(x);
			sb.append("\":");
			sb.append(content);
			BufferedWriter out = new BufferedWriter(new FileWriter(Logger.directory + "/" + key, true),
					content.length() + 32);
			out.write(sb.toString());
			out.close();
		}

		public String get() throws Exception {
			BufferedReader bf = new BufferedReader(new FileReader(key));
			StringBuilder sb = new StringBuilder();
			String line = bf.readLine();
			sb.append("{");
			if (win)
				sb.append("\"type\":\"win\",");
			else if (bid)
				sb.append("\"type\":\"bid\",");
			else
				sb.append("\"type\":\"bid\",");
			sb.append(line);
			sb.append("}");
			bf.close();
			return sb.toString();
		}

		/**
		 * Return the key value of this expiring key
		 * 
		 * @return K. The expiring key's name
		 */
		public String getKey() {
			return key;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final ExpiringKey other = (ExpiringKey) obj;
			if (this.key != other.key && (this.key == null || !this.key.equals(other.key))) {
				return false;
			}
			return true;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int hashCode() {
			int hash = 7;
			hash = 31 * hash + (this.key != null ? this.key.hashCode() : 0);
			return hash;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public long getDelay(TimeUnit unit) {
			return unit.convert(getDelayMillis(), TimeUnit.MILLISECONDS);
		}

		/**
		 * Returns
		 * the time left to live in MS.
		 * 
		 * @return long. The time left.
		 */
		private long getDelayMillis() {
			long z = (startTime + maxLifeTimeMillis) - System.currentTimeMillis();
			if (z < 0)
				z = 0;
			return z;
		}

		/**
		 * Key. Use the form S The time left to live, caclulated from the start
		 * 
		 * @return long. How much longer before timeout, in milliseconds.
		 */
		public long ttl() {
			return System.currentTimeMillis() - startTime;
		}

		/**
		 * Renews the key's start time. Resetting the clock.
		 */
		public void renew() {
			startTime = System.currentTimeMillis();
		}

		/**
		 * Expires this key.
		 */
		public void expire() {
			startTime = System.currentTimeMillis() - maxLifeTimeMillis - 1;
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public int compareTo(Delayed that) {
			return Long.compare(this.getDelayMillis(), ((ExpiringKey) that).getDelayMillis());
		}

		public void setBid(BidResponse bid) throws Exception {
			output("bid", bid);
			this.bid = true;
			complete();
		}

		public void setWin(WinObject win) throws Exception {
			output("win", win);
			this.win = true;
			complete();
		}

		public void setRequest(Map request) throws Exception {
			output("request", map);
			this.request = true;
			complete();
		}

		public boolean complete() {
			if (bid && win && request) {
				expire();
				return true;
			}
			return false;
		}
	}
}
