package com.jacamars.dsp.crosstalk.manager;

import java.nio.file.Files;


import java.nio.file.Paths;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jacamars.dsp.rtb.redisson.RedissonClient;
import com.jacamars.dsp.rtb.commands.BasicCommand;

import com.jacamars.dsp.rtb.tools.NameNode;
import com.jacamars.dsp.rtb.tools.Performance;

/**
 * The main class for Crosstalk
 *
 * @author ben
 */
public class Crosstalk {
    /**
     * The configuration object
     */
    static Configuration config;

    /**
     * Tracks bidders in Aerospike
     */
    static MyBidderTracker tracker;

    /**
     * The object that scans MySql periodically.
     */
    static Scanner scanner;

    /**
     * The list of known campaigns
     */
    List<AccountingCampaign> campaigns;

    /**
     * Log file format string
     */
    static SimpleDateFormat sdf = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * The deadman switch
     */
    static MyControlNode node;

    /**
     * The list of bidders in the system
     */
    public static List<String> bidders = new ArrayList<String>();

    /**
     * Indicates the nedd to refresh all the bidders with all the campaigns.
     */
    public static boolean needMasterUpdate = false;

    /**
     * Is the system ready to takj to the bidders
     */
    public static boolean oldBidState = false;

    /**
     * This class's sl4j log object
     */
    static final Logger logger = LoggerFactory.getLogger(Crosstalk.class);

    /**
     * The /log in memory queues
     */
    public static final List<Deque<String>> deqeues = new ArrayList<Deque<String>>();

    public static void main(String[] args) throws Exception {

        String pidfile = System.getProperty("pidfile");
        if (pidfile != null) {
            String target = System.getProperty("target");
            try {
                String pid = "" + Performance.getPid(target);
                Files.write(Paths.get(pidfile), pid.getBytes());
            } catch (Exception e) {
                System.err.println("WARTNING: Error writing pidfile: " + pidfile);
            }
        }

        AddShutdownHook hook = new AddShutdownHook();
        hook.attachShutDownHook();


        try {
            if (args.length == 0)
                new Crosstalk("config.json");
            else
                new Crosstalk(args[0]);
        } catch (Exception error) {
            error.printStackTrace();
            System.exit(1);
        }

    }

    /**
     * Set the instance name if not already set.
     */
    private void setInstance() {
        if (com.jacamars.dsp.rtb.common.Configuration.instanceName.equals("default") == false)
            return;

        java.net.InetAddress localMachine = null;
        String useName = null;
        try {
            localMachine = java.net.InetAddress.getLocalHost();
            useName = localMachine.getHostName();
        } catch (Exception error) {
            useName = com.jacamars.dsp.rtb.common.Configuration.getIpAddress();
        }

        com.jacamars.dsp.rtb.common.Configuration.instanceName = useName;
    }

    public Crosstalk(String fileName) throws Exception {
        config = Configuration.getInstance();
        config.initialize(fileName, null, null); // initialize the file, but
        // don't add the hooks yet

        ScannerIF scanner = new Scanner();
        Scanner.config = config;


        config.initialize(fileName, scanner, scanner);
        config.connectToSql();

        node = new MyControlNode(config.redisson);

        tracker = new MyBidderTracker(this);

        config.shared.clear();

        setInstance();

        scanner.start();

        if (!scanner.isRunning())
            throw new Exception("Check the logs, Crosstalk failed to start");

        if (Configuration.perfLogs != null) {
            ScheduledExecutorService execService = Executors.newScheduledThreadPool(1);
            execService.scheduleAtFixedRate(() -> {
                logPerformance();
            }, 0L, 60000, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Log the performance data of the system.
     */
    public static void logPerformance() {
        String perf = Performance.getCpuPerfAsString();
        int threads = Performance.getThreadCount();
        String pf = Performance.getPercFreeDisk();
        String mem = Performance.getMemoryUsed();
        long of = Performance.getOpenFileDescriptorCount();
        Map m = new HashMap();
        m.put("timestamp", System.currentTimeMillis());
        m.put("hostname", Configuration.getInstance().config.rtbName);
        m.put("openfiles", of);
        m.put("cpu", Double.parseDouble(perf));

        String[] parts = mem.split("M");
        m.put("memused", Double.parseDouble(parts[0]));
        parts[1] = parts[1].substring(1, parts[1].length() - 2);
        parts[1] = parts[1].replaceAll("\\(", "");
        m.put("percmemused", Double.parseDouble(parts[1]));

        m.put("freedisk", Double.parseDouble(pf));
        m.put("campaigns", Scanner.campaigns.size());
        m.put("loglags", Scanner.budgets.deltaTime());
        try {
            m.put("dailyspend", Scanner.budgets.getCampaignDailySpend(null));
            m.put("spendratepermin", Scanner.budgets.getCampaignSpendAverage(null));
        } catch (Exception error) {
            logger.error("Error getting spends: {}", error.toString());
        }
        Configuration.perfLogs.add(m);
    }

    /**
     * Are we in the bid state.
     *
     * @return bolean. Returns true if bidders are available, else returns false.
     */
    public static boolean inBidState() {
        if (bidders.size() == 0) {
            return false;
        }

        return true;
    }

}

/**
 * This bidder's instance of name node
 *
 * @author Ben M. Faul
 * B
 */
class MyControlNode extends NameNode {

    public MyControlNode(RedissonClient client) throws Exception {
        super(client);
    }

}

/**
 * Tracks all the bidders
 */
class MyBidderTracker implements Runnable {
    Thread me;
    Crosstalk parent;
    List<String> bidders;
    List<String> prev;
    boolean first = true;
    RedissonClient redisson;
    static final String BIDDERCACHE2K = "bidderscache";        // used for cache2k

    /**
     * Track the bidders.
     * @param parent Crosstalk. The owner of this object
     */
    public MyBidderTracker(Crosstalk parent) {
        redisson = Configuration.getInstance().redisson;
        this.parent = parent;
        me = new Thread(this);
        me.start();
    }

    /**
     * Remove a bidder, happens on a shutdown.
     * @param who String. The instance name of whom to delete.
     * @throws Exception on redisson errors.
     */
    public void remove(String who) throws Exception {
        parent.node.remove(who);
    }

    /**
     * Check to see if a bidder is new
     * @param msg BasicCommand. The command that triggered the check.
     * @throws Exception on redisson errors.
     */
    public void checkFor(BasicCommand msg) throws Exception {
        String from = msg.from;
        if (parent.node.isMember(from)) {
            parent.node.addMember(from);    // will update the time stamp
            return;
        } else {
            parent.logger.info("Adding a previously unknown bidder: {}", from);
            parent.node.addMember(from);
        }
        check();
    }

    /**
     * Run the application, periodic processing
     */
    public void run() {

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                check();
                first = false;
                Thread.sleep(5000);

            } catch (Exception error) {
                error.printStackTrace();
                return;
            }
        }
    }

    /**
     * Check on the bidders to see if there are new ones, and, if old ones have stopped.
     * @throws Exception on redisson errors.
     */
    public void check() throws Exception {

        try {
            bidders = Crosstalk.node.getMembers();
            if (bidders == null)
                bidders = new ArrayList<String>();
        } catch (Exception error) {
            System.err.println("Redisson error getting bidders, continuing");
            error.printStackTrace();
            return;
        }

        if (bidders.isEmpty() && !parent.needMasterUpdate) {
            parent.logger.warn("Warning, no bidders are running!");
            parent.needMasterUpdate = true;
        }

        if (first)
            parent.logger.info("These bidders are apparently running: {} ", bidders);
        else {
            if (!bidders.containsAll(parent.bidders)
                    || !parent.bidders.containsAll(bidders)) {
                if (bidders.size() > parent.bidders.size()) {
                    parent.logger.info("More bidders are on-line these bidders are apparently running: {}, scheduling update", bidders);
                    for (String bidder : bidders) {
                        if (!parent.bidders.contains(bidder)) {
                            Scanner.getInstance().refreshBidder(bidder);
                        }
                    }
                    parent.needMasterUpdate = true;

                } else if (parent.bidders.size() < bidders.size()) {
                    parent.logger.info("Some bidders have dropped off line, these bidders are apparently running: {}",
                            bidders);
                } else if (parent.bidders.size() == bidders.size()) {
                    parent.logger.info("A change in bidders has occurred, these are running: {}", bidders);
                    parent.needMasterUpdate = true;
                }
                if (parent.needMasterUpdate)
                    Scanner.forceRun = true;
            }
        }

        parent.bidders.clear();
        for (String bidder : bidders) {
            parent.bidders.add(bidder);
        }
    }
}

/**
 * Add a shutdown hook to catch control-c and other kill signals.
 *
 * @author Ben M. Faul
 */
class AddShutdownHook {
    public void attachShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Inside Add Shutdown Hook");
                try {
                    Configuration.getInstance().deadmanSwitch.deleteKey();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Crosstalk.scanner.panicStopBidders("Program abort");
            }
        });
        System.out.println("*** Shut Down Hook Attached. ***");
    }
}
