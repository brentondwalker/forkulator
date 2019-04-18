package forkulator;

/**
 * Abstract superclass for all event types handled by the simulator.
 * 
 * @author brenton
 *
 */
public abstract class QEvent implements Comparable<QEvent> {

	public double time = 0.0;
	
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

