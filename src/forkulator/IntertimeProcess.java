package forkulator;

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
	
	public abstract double nextInterval();
	
}
