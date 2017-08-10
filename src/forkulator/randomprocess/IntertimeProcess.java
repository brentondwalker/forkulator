package forkulator.randomprocess;

import java.util.Random;

/**
 * This interface represents a random process of things arriving or being
 * serviced.  I defines the basic API where the user can ask: "When will the
 * next thing happen?"
 * 
 * That thing could be arrivals or services, and the implementing class
 * could be constrained above or below or both.  This just defines the API.
 * 
 * @author brenton
 *
 */
public abstract class IntertimeProcess {
	
	protected static Random rand = new Random();
	
	/**
	 * This method should be called for getting inter-arrival times.
	 * If you omit the jobSize it will default to 1.
	 * 
	 * @param jobSize
	 * @return
	 */
	public double nextInterval(int jobSize) {
		throw new UnsupportedOperationException("ERROR: method not implemeted");
	}
	
	public double nextInterval() {
		return nextInterval(1);
	}
	
	/**
	 * This method should be called for getting service times.
	 * Because service processes can be idle, you need to pass in
	 * the current time so the process knows what time "now" is.
	 * 
	 * @param curentTime
	 * @return
	 */
	public double nextInterval(double curentTime) {
		throw new UnsupportedOperationException("ERROR: method not implemeted");
	}
	
	/**
	 * This returns a new instance of whatever type of IntertimeProcess you have,
	 * configured with the same parameters.
	 */
	public abstract IntertimeProcess clone();
	
	/**
	 * return a tab-separated string containing the processes parameters
	 * 
	 * @return
	 */
	public abstract String processParameters();
	
}
