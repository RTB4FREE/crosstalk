package com.jacamars.dsp.crosstalk.manager;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.jacamars.dsp.rtb.redisson.RedissonClient;
import com.jacamars.dsp.rtb.commands.BasicCommand;
import com.jacamars.dsp.rtb.commands.Echo;

public enum Cache2kProcessor {
	INSTANCE;

	static Thread me;
	static RedissonClient redisson;
	static final List<Map> bidders = new ArrayList<Map>();

	public static Cache2kProcessor getInstance() {
		if (redisson == null) {
			redisson = Configuration.getInstance().redisson;

			Runnable updater = () -> {
				BasicCommand echo = new Echo();
				echo.from = "deadmanswitch";
				while (true) {
					try {
						Thread.sleep(5000);
						Configuration.commandsQueue.add(echo);
						ageBidders();
						;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			};
			Thread nthread = new Thread(updater);
			nthread.start();
		}

		return INSTANCE;
	}

	public void updateMap(BasicCommand cmd) {
		String what = cmd.from;
		synchronized (bidders) {
			for (int i = 0; i < bidders.size(); i++) {
				Map<String, Comparable> x = bidders.get(i);
				String check = (String) x.get("name");
				if (check.equals(what)) {
					int value = 4;
					x.put("koutofn", value);
					bidders.remove(i);
					bidders.add(x);
					//System.out.println("====> " + what + " koutofn = " + value);
					return;
				}
			}
			Map<String, Comparable> m = new HashMap<String, Comparable>();
			m.put("name", what);
			Integer start = new Integer(4);
			m.put("koutofn", start);
			bidders.add(m);
		}
	}
	
	public void remove(String x) {
		synchronized (bidders) {
			Iterator<Map> i = bidders.iterator();
			while (i.hasNext()) {
				Map<?, ?> m = i.next(); // must be called before you can call
									// i.remove()
				String who = (String)m.get("name");
				if (who.equals(x)) {
					//System.out.println("====> DELETE: " + who);
					i.remove();
					return;
				}
			}
		}
	}

	/**
	 * Age the bidders
	 */
	public static void ageBidders() {
		synchronized (bidders) {
			Iterator<Map> i = bidders.iterator();
			while (i.hasNext()) {
				Map<String, Integer> m = i.next(); // must be called before you can call
									// i.remove()
				int v = (Integer) m.get("koutofn");
				v--;
				// System.out.println("====> " + m.get("name") + " koutofn = " + v);
				if (v == 0) {
					i.remove();
					System.out.println("====> " + m.get("name") + " REMOVED!");
				}
				else
					m.put("koutofn", v);
			}
		}

	}

	/**
	 * Return a list of bidders in the system
	 * 
	 * @return List. A list of running bidders
	 */
	public List<String> getBidders() {
		List<String> list = new ArrayList<String>();
		synchronized (bidders) {
			for (Map<?, ?> m : bidders) {
				String name = (String) m.get("name");
				list.add(name);
			}
		}
		return list;
	}
}
