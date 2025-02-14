package forkulator;

/**
 * Abstract superclass for all event types handled by the simulator.
 * 
 * @author brenton
 *
 */
public abstract class QEvent implements Comparable<QEvent> {

    /*
     * The time of the event.
     */
	public double time = 0.0;
	
	/*
	 * Tell the simulator to ignore this event
	 * This should be more efficient than trying to pull events out of the queue when a job or task is killed
	 * */
	public boolean deleted = false;

	public int compareTo(QEvent ee)
    {
	    double diff = this.time - ee.time;
        if (diff < 0.0d) {
            return -1;
        } else if (diff > 0.0d) {
            return 1;
        }
        return 0;
    }
	
}

