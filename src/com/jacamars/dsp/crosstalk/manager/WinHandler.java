package com.jacamars.dsp.crosstalk.manager;

import java.io.BufferedWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.rtb.pojo.WinObject;

/**
 * A File handler for win notification messages, sharable by multiple threads.
 * 
 * @author Ben M. Faul
 *
 */
public class WinHandler {
	/** The contents that is written to disk */
	final StringBuilder sb = new StringBuilder();
	
	/** The formatting object used for the rollover */
	final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
	
	/** The object serializer */
	final ObjectMapper mapper = new ObjectMapper();

	/** The buffered writer used to send the data out */
	volatile BufferedWriter bw;

	/** The log's name. */
	private final String name;

	/**
	 * Constructor for base class.
	 *
	 * @param name String. The name of the log
	 * @throws Exception
	 *             s on Network errors
	 */
	public WinHandler(String name) throws Exception {
		this.name = name;
		ScheduledExecutorService execService1 = Executors.newScheduledThreadPool(1);
		execService1.scheduleAtFixedRate(() -> {
			try {
				setTime();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 0, 10, TimeUnit.MINUTES);

		ScheduledExecutorService execService2 = Executors.newScheduledThreadPool(1);
		execService2.scheduleAtFixedRate(() -> {
			try {
				synchronized (sb) {
					boolean wrote = false;
					if (sb.length() > 0) {
						try {
							bw.append(sb);
							sb.setLength(0);
							wrote = true;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (wrote)
						bw.flush();
				}
				;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 1, 1, TimeUnit.SECONDS);

	}

	/**
	 * Add a win object to the queue.
	 * 
	 * @param e
	 *            WinObject. The new object to put into the queue.
	 */
	public void add(WinObject e) {
		try {
			String content = mapper.writer().writeValueAsString(e);
			synchronized (sb) {
				sb.append(content);
				sb.append("\n");
			}
		} catch (Exception error) {
			error.printStackTrace();
		}
	}

	void setTime() {
		final String tailstamp = "-" + sdf.format(new Date());
		try {
			final String fileName = name + tailstamp;
			FileWriter x = new FileWriter(fileName, true);
			synchronized (sb) {
				if (bw != null) {
					if (sb.length() > 0)
						bw.append(sb);
					sb.setLength(0);
					bw.close();
				}
				bw = new BufferedWriter(x);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}