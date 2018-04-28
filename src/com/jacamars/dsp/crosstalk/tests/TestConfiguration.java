package com.jacamars.dsp.crosstalk.tests;

import static org.junit.Assert.fail;



import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;
import com.jacamars.dsp.crosstalk.manager.Configuration;
import com.jacamars.dsp.crosstalk.manager.ScannerIF;
import com.jacamars.dsp.rtb.commands.BasicCommand;

import com.jacamars.dsp.rtb.fraud.FraudLog;

public class TestConfiguration implements ScannerIF {
	CountDownLatch latch;
	
	@BeforeClass
	public static void setup() {
		try {

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@AfterClass
	public static void stop() {

	}
	
	
	/**
	 * Test Configuration and setup;
	 */
	@Test
	public void testSetup() {
		Configuration c = Configuration.getInstance();
		try {
			c.initialize("config.json", this, this);
		} catch (Exception e) {
			e.printStackTrace();
			fail("Bad configuration, fix this first");
		}
		
	
		
		
		//
		// Test the sql connection
		//
		Statement statement = c.getStatement();
		try {
			ResultSet resultSet = statement.executeQuery("select * from campaigns");
		} catch (SQLException e) {
			e.printStackTrace();
			fail("The campaigns table is NULL!");
		}
	
		
		System.out.println("\nA star is born!");

	}

	
	@Override
	public void callBack(BasicCommand msg) {
		latch.countDown();
	}

	@Override
	public AccountingCampaign makeNewCampaign(ObjectNode node) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean isRunning() {
		// TODO Auto-generated method stub
		return false;
	}

}
