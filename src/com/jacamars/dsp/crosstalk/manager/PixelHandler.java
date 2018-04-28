package com.jacamars.dsp.crosstalk.manager;

import java.io.BufferedWriter;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jacamars.dsp.rtb.commands.PixelClickConvertLog;
import com.jacamars.dsp.rtb.pojo.WinObject;

/**
 * Handles the pixels and clicks logs. Just pass the directory, the class will create clicks-* and pixels-* as needed.
 * 
 * @author Ben M. Faul
 *
 */
public class PixelHandler {
	/** JSON serialization object */
	private final String pixelName;
	private final String clickName;
	private final StringBuilder sbPixels = new StringBuilder();
	private final StringBuilder sbClicks = new StringBuilder();
	private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd-HH:mm");
	private final ObjectMapper mapper = new ObjectMapper();

	private BufferedWriter bwClicks;
	private BufferedWriter bwPixels;
	private String pixelsFileName;
	private String clicksFileName;
	
	/** This class's sl4j logger */
	static final Logger logger = LoggerFactory.getLogger(PixelHandler.class);

	/**
	 * Constructor the pixel and click handler. Just pass the log directory. It will
	 *
	 * @throws Exception
	 *             s on Network errors
	 */
	public PixelHandler(String name) throws Exception {
		pixelName = name + "/pixels";
		clickName = name + "/clicks";
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
				synchronized (sbPixels) {
					boolean wrotePixels = false;
					if (sbPixels.length() > 0) {
						try {
                            bwPixels.append(sbPixels);
							sbPixels.setLength(0);
							wrotePixels = true;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (wrotePixels)
						bwPixels.flush();
				}

				synchronized (sbClicks) {
					boolean wroteClicks = false;
					if (sbClicks.length() > 0) {
						try {
                            bwClicks.append(sbClicks);
							sbClicks.setLength(0);
							wroteClicks = true;
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					if (wroteClicks)
						bwClicks.flush();
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
	public void add(PixelClickConvertLog e) {
		try {
			String content = mapper.writer().writeValueAsString(e);
			if (e.type == PixelClickConvertLog.CLICK) {
				synchronized (sbClicks) {
					sbClicks.append(content);
					sbClicks.append("\n");
				}
			} else {
				synchronized (sbPixels) {
					sbPixels.append(content);
					sbPixels.append("\n");
				}
			}
		} catch (Exception error) {
			logger.error("Error handling pixel '{}', error: {}", e.payload,error.toString());
			error.printStackTrace();
		}
	}

	/**
	 * Set time has arrived to write the StringBuilder to file
	 */
	void setTime() {
		final String tailstamp = "-" + sdf.format(new Date());
		try {
			pixelsFileName = pixelName + tailstamp;
			FileWriter x = new FileWriter(pixelsFileName, true);

			synchronized (sbPixels) {
				if (bwPixels != null) {
					if (sbPixels.length() > 0)
						bwPixels.write(sbPixels.toString());
					sbPixels.setLength(0);
					bwPixels.close();
				}
			}
			bwPixels = new BufferedWriter(x);

			clicksFileName = clickName + tailstamp;
			x = new FileWriter(clicksFileName, true);

			synchronized (sbClicks) {
				if (bwClicks != null) {
					if (sbClicks.length() > 0)
						bwClicks.write(sbClicks.toString());
					sbClicks.setLength(0);
					bwClicks.close();
				}
			}
			bwClicks = new BufferedWriter(x);

		} catch (Exception e) {
			logger.error("Error wiring file {}. : {}",pixelsFileName, e.toString());
			e.printStackTrace();
		}
	}
}