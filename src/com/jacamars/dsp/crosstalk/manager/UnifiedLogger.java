package com.jacamars.dsp.crosstalk.manager;

import java.util.Map;




import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.jacamars.dsp.rtb.commands.PixelClickConvertLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;

import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import com.google.common.cache.RemovalNotification;

import com.jacamars.dsp.rtb.bidder.ZPublisher;
import com.jacamars.dsp.rtb.pojo.BidResponse;
import com.jacamars.dsp.rtb.pojo.WinObject;

/**
 * Unified in-memory logger. Batches up requests, bids, wins, and clicks into a single log entry. Evicts (writes to log)
 * once the terminal event happens or the timeout is reached on the cache. The cache will automatically expire on the 
 * events. When they expire, they are written to the Publisher object.
 * @author Ben M. Faul
 *
 */

public class UnifiedLogger  {
	static final int EXPIRE_BID = 0;
	static final int EXPIRE_WIN = 1;
	static final int EXPIRE_CLICK = 2;

	int expireOn = EXPIRE_WIN;
	ExecutorService executorService = Executors.newFixedThreadPool(32);
	volatile LoadingCache<String, Graph> graphs;
	ZPublisher publisher;
	ScheduledExecutorService execService;
	
	
	static final Logger logger = LoggerFactory.getLogger(UnifiedLogger.class);

	public UnifiedLogger(int timeout, ZPublisher publisher) {
		final ZPublisher pub = publisher;
		this.publisher = publisher;
		
		RemovalListener<String, Graph> removalListener = new RemovalListener<String, Graph>() {
			public void onRemoval(RemovalNotification<String, Graph> removal) {
				if (pub != null) {
					pub.add(removal.getValue());
				}
				//System.out.println("Eviction, Value = " + removal.getValue().toString());
			}
		};
		RemovalListeners.asynchronous(removalListener, executorService);

		graphs = CacheBuilder.newBuilder().expireAfterWrite(timeout , TimeUnit.MINUTES).removalListener(removalListener)
				.build(new CacheLoader<String, Graph>() {
					public Graph load(String key) { // no checked exception
						return createExpensiveGraph(key);
					}
				});

		execService = Executors.newScheduledThreadPool(5);
		execService.scheduleAtFixedRate(() -> {
			try {
				cleanup();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 5000L, 5000L, TimeUnit.MILLISECONDS);

		logger.info("Unified logger is running.");
	}
	
	/**
	 * Shut down the logger. Will emit all the messages so far queued, even if not completed.
	 */
	public void shutdown() {
		execService.shutdownNow();
		try {
			execService.awaitTermination(1000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			logger.error("Error shutting down service: " + e);
		}
		Map<String, Graph> m = graphs.asMap();
		m.forEach((k,v)->publisher.add(v.toString()));
	}

	/**
	 * Set to expire on bids.
	 */
	public void expireOnBid() {
		expireOn = EXPIRE_BID;
	}

	/**
	 * Set to expire on wins.
	 */
	public void expireOnWin() {
		expireOn = EXPIRE_WIN;
	}

	/**
	 * Set to expire on clicks
	 */
	public void expireOnClick() {
		expireOn = EXPIRE_CLICK;
	}

	/**
	 * Cleanup the graphs (clean out all the expired logs).
	 * @throws Exception on JSON and I/O errors.
	 */
	public void cleanup() throws Exception {
		graphs.cleanUp();
	}

	/**
	 * Invalidates a log. Deleting an entry makes it eligible for writing to the log
	 * @param id String. The object's id.
	 * @throws Exception on Cache errors.
	 */
	public void remove(String id) throws Exception {
		graphs.invalidate(id);
	}

	/**
	 * Read a graphed log from the cache.
	 * @param id String. The id of the thing to load.
	 * @return Graph. The graphed log.
	 * @throws Exception on Cache errors.
	 */ 
	public Graph read(String id) throws Exception {
		return graphs.get(id);
	}

	/**
	 * Add a request object to the log.
	 * @param oid String. The bid id
	 * @param request BidRequest. The actual bid request object.
	 * @throws Exception on cache errors.
	 */
	public void addRequest(String oid, Map request) throws Exception {
		Graph g = null;
		request.remove("type");     // redundant
		try {
			g = graphs.get(oid);
			g.update(request);
			//System.out.println("Adding request to previously created record " + oid + " = " + request);
		} catch (InvalidCacheLoadException e) {
			// System.out.println("Adding new request " + oid + " = " + request);
			g = new Graph(request);
			graphs.put(oid, g);
		} finally {
			
		}

	}

	/**
	 * Add a bid response to the cache.
	 * @param oid String. The id of the bid.
	 * @param request BidResponse. The bid object
	 * @throws Exception on cache errors.
	 */
	public void addResponse(String oid, BidResponse request) throws Exception {
		Graph g = null;
		try {
			g = graphs.get(oid);
			g.update(request);
			//System.out.println("Adding response " + oid + " = " + request);
		} catch (Exception error) {
			//System.out.println("New Graph needed for response " + oid);
			g = new Graph(request);
			graphs.put(oid, g);
		}
		
		if (expireOn == EXPIRE_BID)
			graphs.invalidate(oid);

	}

	/**
	 * Add a win record to the cache.
	 * @param oid String. The object id.
	 * @param win WinObject. The actual win object to add to the log.
	 * @throws Exception on cache errors.
	 */
	public void addWin(String oid, WinObject win) throws Exception {
		Graph g;
		try {
			g = graphs.get(oid);
			logger.debug("Adding pid = {} win {}",oid,win);
		} catch (Exception error) {
			logger.error("NO SUCH REQUEST RECORD: {}",oid);
			g = new Graph(win);
			graphs.put(oid,g);
		}
		if (expireOn == EXPIRE_WIN) {
			if (logger.isDebugEnabled()) {
				logger.debug("Invalidating win " + oid);
			}
			graphs.invalidate(oid);
		}
	}

	/**
	 * Add a click message to the log.
	 * @param oid String. The id of the bid.
	 * @param click ClickLog. The logged click object.
	 * @throws Exception on cache errors.
	 */
	public void addClick(String oid, PixelClickConvertLog click) throws Exception {
		Graph g = graphs.get(oid);
		if (g != null) {
			g.update(click);
			if (expireOn == EXPIRE_CLICK)
				graphs.invalidate(oid);
		} else {
			logger.warn("Click not procecced {} already evicted.", oid);
		}
			
		
	}

	/**
	 * Create the graph object
	 * @param key String. Unused.
	 * @return null.
	 */
	protected Graph createExpensiveGraph(String key) {
		return null;
	}
}

/**
 * The internal graphed log
 * @author Ben M. Faul
 *
 */
class Graph {
	public static final String REQUEST = "request";
	public static final String BID = "bid";
	public static final String WIN = "win";
	public static final String CLICK = "click";

	public static ObjectMapper mapper = new ObjectMapper();
	static {
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@JsonProperty
	public volatile String type = REQUEST;
	@JsonProperty
	volatile Map request;
	@JsonProperty
	volatile BidResponse response;
	@JsonProperty
	volatile WinObject win;
	@JsonProperty
	volatile PixelClickConvertLog click;

	public Graph() {

	}

	public Graph(Map request) {
		this.request = request;
	}
	
	public Graph(BidResponse bid) {
		this.response = bid;
		type = BID;
	}
	
	public Graph(WinObject win) {
		this.win = win;
		type = WIN;
	}
	
	public void update(Map request) {
		this.request = request;
		// don't set the type, a subsequent message arrived first.
	}

	public void update(BidResponse response) {
		this.response = response;
		type = BID;
	}

	public void update(WinObject win) {
		this.win = win;
		type = WIN;
	}

	public void update(PixelClickConvertLog click) {
		this.click = click;
		type = CLICK;
	}

	public String toString() {
		String content = null;
		try {
			content = mapper.writer().withDefaultPrettyPrinter().writeValueAsString(this);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return content;
	}

}