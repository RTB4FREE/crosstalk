package com.jacamars.dsp.crosstalk.tests;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.jacamars.dsp.crosstalk.manager.AtomicBigDecimal;


public class TestAtomicBigDecimal  {

	static AtomicBigDecimal decimal = new AtomicBigDecimal();
	
	@Test
	public void testFiveThreads() {
		
		CountDownLatch latch = new CountDownLatch(5);
		CountDownLatch flag = new CountDownLatch(1);
		
		for (int i=0;i<5; i++) {
			Runnable runner = () -> {
				try {
					flag.await();
					Usable a = new Usable(decimal);
					latch.countDown();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
			Thread nthread = new Thread(runner);
			nthread.start();
		}
		flag.countDown();
		try {
			latch.await();
			long value = decimal.get().longValue();
			assertTrue(500==value);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

class Usable { // Note Java Code Convention, also class name should be meaningful   
    private int i;
    public Usable(AtomicBigDecimal d) {
    	
    	for (int i=0; i<100;i++) {
    		d.incrementAndGet();
    	}
    }
}
	
