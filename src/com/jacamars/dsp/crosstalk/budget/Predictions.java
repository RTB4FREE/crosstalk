package com.jacamars.dsp.crosstalk.budget;

import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jacamars.dsp.crosstalk.manager.AccountingCampaign;

/**
 * Predictive algorithms for spend analysis
 * @author Ben M. Faul
 *
 */
public class Predictions {
	
	// Hours past 0 hour
	double hoursPastZero;
	// minutes past last hour
	double minutesPastZero;
	
	static final Logger logger = LoggerFactory.getLogger(Predictions.class);
	
	/**
	 * Set the scaling factors
	 */
	public void setScale() {
		Calendar calendar = Calendar.getInstance();
		hoursPastZero = calendar.get(Calendar.HOUR_OF_DAY);
		minutesPastZero = calendar.get(Calendar.MINUTE);
		minutesPastZero += (double)calendar.get(Calendar.SECOND)/60.0;
		hoursPastZero += minutesPastZero/60;
	}
	
	/**
	 * Get the effective spend at the current bidding rate and lag/
	 * @param spend double. The current known spend.
	 * @param lagSeconds double. The number of seconds the log is behind.
	 * @param ratePerMinute double. The spend rate per minute
	 * @return double. The effective spend, taking lag and spend rate into consideration.
	 */
	public double getEffectiveSpend(double spend, double lagSeconds, double ratePerMinute) {
		double total = spend + (lagSeconds * ratePerMinute/60);
		return total;
	}
	
	/**
	 * How many minutes until we bust the hourly budget
	 * @param budget double. The budget for this hour.
	 * @param current double. The current hourly total (note this is scaled at minutes past the hour).
	 * @param ratePerMinute double. The spend rate per minute.
	 * @return the number of minutes before busting the hourly budget, or -1 if we don't think we will bust it.
	 */
	public double bustHour(double budget, double current, double ratePerMinute) {
		double scaleH = minutesPastZero / 60;
		double finalExpected = current / scaleH;
		
		if (finalExpected <= budget)
			return -1;
		
		double underrun = budget - current;
		double minsToOverrun = underrun / ratePerMinute;
		
		if ((60 - minutesPastZero) < minsToOverrun)
			return -1;
		
		return minsToOverrun;
	}
	
	/**
	 * How many minutes until we bust the day budget.
	 * @param budget double. The budget.
	 * @param current double. The current spend, scaled at hours past 0.
	 * @param ratePerMinute diuble. The spend rate per minute.
	 * @return double. The number of minutes until we bust the budget, or -1 if we predict we won't make it.
	 */
	public double bustDay(double budget, double current, double ratePerMinute) {
		double scaleH = hoursPastZero / 24;
		double finalExpected = current / scaleH;
		
		if (finalExpected <= budget)
			return -1;
		
		double underrun = budget - current;
		double minsToOverrun = underrun / ratePerMinute;
		double hoursToOverrun = minsToOverrun/60;
		
		if ((24 - hoursPastZero) < hoursToOverrun)
			return -1;
		
		return minsToOverrun;
	}
	
	/**
	 * Return the time at which we believe will make the budget.
	 * @param total double. The current total spend.
	 * @param budget double. The budget.
	 * @param rate double. The rate of spend per minute.
	 * @return long. The epoch of when we will make bank.
	 */
	public long makeBank(double total, double budget, double rate) {
		double scale = 1;
		double finalExpected = budget;
		
		double underrun = budget - total;
		double minsToUnderrun = underrun / rate;
		
		long now = System.currentTimeMillis();
		return now + (long)(minsToUnderrun * 60) * 1000;
	}
	
	public void main(String args[]) {
		Predictions p = new Predictions();
		p.minutesPastZero = 30;
		p.hoursPastZero = 12;
		double budget = 100;
		double current = 51;
		double rate = 1.7;
		
		double x = p.bustHour(budget, 63, rate);
		System.out.println("Mins: " + x);
		
		double h = p.bustDay(100.0, 62, .069);
		System.out.println("Mins: " + h);
		
		long z  = p.makeBank(51,100,.069);
		System.out.println(z);
		Calendar cal = Calendar.getInstance();
		Date now = new Date();
		now.setTime(z);
		System.out.println(now);
		
		System.out.println("EffectiveSpend: " + p.getEffectiveSpend(100,300/60,.35));
	}
}
