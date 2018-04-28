package com.jacamars.dsp.crosstalk.budget;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Compute moving average using a sliding window. The inputs are totals, and an entry is actually the value
 * add(a) - add(b). Which is equivalent to adding (Math.abs(a-b))
 * @author Ben M. Faul
 *
 */
public class MovingAverage {

	
    /** the max size of the window */
    private int size = 10;
    
	/** The sliding window */
    private  double samples[] = new double[size];
    
    /** The last value added, -1 means no entry */
    private double last = -1;
    
    /** Running total */
    private double total = 0d;

    /** The sliding window index */
    private int index = 0;
    
    /** Last presize before size is reached */
    private int last_size = 0;
    
    /**
     * Create the moving average object.
     * @param size int. The size of the window.
     */
    public MovingAverage(int size) {
    	this.size = size;
    	samples = new double[size];

    }
    
    /**
     * Constructor for JSON ro use.
     */
    public MovingAverage() {
    
    }

    
    /**
     * Add a total number. What gets added is the absolute value of the previous number - this number.
     * @param number double. The number to add.
     */
    public void add(double x) {
    	if (last_size < size)
    		last_size++;
    	
    	if (last == -1) {
    		last = x;
    		return;
    	};
    	double save = x;
    	x = Math.abs(last - x);
    	last = save;
    	total -= samples[index];
        samples[index] = x;
        total += x;
        if (++index == size) index = 0; // cheaper than modulus
    }

    /**
     * Get the moving average over the sliding window.
     * @return double. The moving average.
     */
    public double getAverage() {
    	return total / last_size;
    }
    
    
    /**
     * Get the standard deviation for the spend rate average. Must have a minimum of 2 samples.
     * @return double. The standard deviation for this mobing average.
     */
    public double getStd () {
    	if (last_size < 2)
    		return Double.NaN;
        return Math.sqrt( total / ( last_size - 1 ) ); // sample
    }
    
    public static void main(String [] args) {
    	MovingAverage x = new MovingAverage(10);
    	x.add(0.17876499999999942);
    	System.out.println(x.getAverage());
    	x.add(.18445624999999946);
    	System.out.println(x.getAverage());
    	x.add(0.1719128571428567);
    	System.out.println(x.getAverage());
    	x.add(0.15909749999999956);
    	System.out.println(x.getAverage());
    	x.add(0.13243599999999942);
    	System.out.println(x.getAverage());
   
    	System.out.println(x.getStd());
    }

}
