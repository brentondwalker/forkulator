package forkulator;

import java.io.Serializable;
import java.util.Queue;

public class FJWorker implements Serializable {
	
	/**
	 * Supposed to add this to say the class implements Serializable.
	 */
	private static final long serialVersionUID = 1L;
	
	public int stage = 0;
	
	public FJTask current_task = null;
	
	public Queue<FJTask> queue = null;
	
	public FJWorker(int stage) {
		this.stage = stage;
	}
	
}
