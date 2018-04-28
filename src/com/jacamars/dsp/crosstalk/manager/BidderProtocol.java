package com.jacamars.dsp.crosstalk.manager;

import com.jacamars.dsp.rtb.commands.BasicCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by ben on 8/14/17.
 */
public class BidderProtocol {
    /** This class's sl4j logger object */
    static final Logger logger = LoggerFactory.getLogger(BidderProtocol.class);
    public static final Set<BidderProtocol> bidders = ConcurrentHashMap.newKeySet();

    String name;
    Set<Command> outstanding =  ConcurrentHashMap.newKeySet();

    public BidderProtocol(String name) {
        this.name = name;
    }

    public void send(BasicCommand cmd) {
        for (Command c : outstanding) {
            if (c.cmd.id.equals(cmd.id))
                return;
        }

        if (cmd.to.equals("") || cmd.to.equals("*") || cmd.to.equals(name)) {
            outstanding.add(new Command(this,name,cmd));
        }

    }

    void ack(BasicCommand cmd) {
        for (Command c : outstanding) {
            if (c.cmd.id.equals(cmd.id)) {
                c.ack(cmd);
                return;
            }
        }
    }

    public void interrupt() {
        for (Command c : outstanding) {
            c.interrupt();
        }

        BidderProtocol.bidders.remove(this);
        Crosstalk.bidders.remove(name);
        BidderProtocol.logger.error("---- TIMEOUT ON {} THIS IS A SERIOUS ERROR -----", name);
    }

    public void removeCommand(Command c) {
        outstanding.remove(c);
    }

    public static void process(BasicCommand cmd) {
        /**
         * If this is "" or "*", then for each bidder, make a protocol for it, if one does not exist
         */
        for (String name : Crosstalk.bidders) {
            BidderProtocol p = getProtocol(name);
            if (p == null) {
                p = new BidderProtocol(name);
                p.send(cmd);
                bidders.add(p);
            }
        }

        for (BidderProtocol p : bidders) {
            p.send(cmd);
        }

    }

    public static BidderProtocol getProtocol(String name) {
        for (BidderProtocol p : bidders) {
            if (p.name.equals(name)) {
                return p;
            }
        }
        return null;
    }

    public static void addProtocol(BidderProtocol p) {
        bidders.add(p);
    }

    public static boolean hasOutstandingCommands(String name) {
        for (BidderProtocol p : bidders) {
            if (p.name.equals(name)) {
                if (p.outstanding.size() != 0)
                    return true;
                else
                    return false;
            }
        }
        return false;
    }

    public static void processAck(BasicCommand cmd) {
        for (BidderProtocol p : bidders) {
            Runnable updater;
            updater = () -> {
                p.ack(cmd);
            };
            Thread thread = new Thread(updater);
            thread.start();

        }
    }

    public static void removeBidder(String name) {
        for (BidderProtocol p : bidders) {
            if (p.name.equals(name)) {
                p.interrupt();
                bidders.remove(p);
                return;
            }
        }
    }
}

class Command {
    int retries = 0;
    BasicCommand cmd;
    ScheduledExecutorService execService;
    BidderProtocol parent;

    public Command(BidderProtocol parent, String name, BasicCommand cmd) {
        this.parent = parent;
        BasicCommand copy = new BasicCommand();
        copy.cmd = cmd.cmd;
        copy.from = cmd.from;
        copy.id = cmd.id;
        copy.to = name;
        copy.msg = cmd.msg;
        copy.name = cmd.name;
        copy.price = cmd.price;
        copy.target = cmd.target;
        copy.status = cmd.status;
        copy.timestamp = cmd.timestamp;

        this.cmd = copy;

        execService = Executors.newScheduledThreadPool(1);
        execService.scheduleAtFixedRate(() -> {
            try {
                check();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }

    public void ack(BasicCommand test) {
        BidderProtocol.logger.debug("Checking test id: {} vs command id: {}", test.id, cmd.id);
        if (test.id.equals(cmd.id)) {
            BidderProtocol.logger.debug("---- ACK RECECIVED {} -----> {}", cmd.id, test.from);
            interrupt();
        }
    }

    public void check() {
        if (retries < 4) {
            retries++;
            String to = cmd.to;
            if (to.equals(""))
                to = "*";

            cmd.timestamp = System.currentTimeMillis();
            String name = "" + cmd.cmd;

            if (cmd.cmd == 0)
                name = "ADD CAMPAIGN";
            if (cmd.cmd == 1)
                name = "DELETE CAMPAIGN";
            if (cmd.cmd == 2)
                name = "STOP BIDDERS";
            if (cmd.cmd == 3)
                name = "START BIDDERS";

            BidderProtocol.logger.debug("---- RETRY {} -----> {}:{} {}", retries, name, cmd.id , cmd.target);
            Configuration.getInstance().commandsQueue.add(cmd);
            retries++;
        } else {
            retries = 0;
            BidderProtocol.logger.error("---- COMMAND {} TIMED OUT TO {} --------", cmd, cmd.from);
        }
    }

    public void interrupt() {
        execService.shutdownNow();
        parent.removeCommand(this);
    }
}
