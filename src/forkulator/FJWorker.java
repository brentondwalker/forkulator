package forkulator;

import java.util.Queue;

public class FJWorker {
		
	public int stage = 0;
	
	public FJTask current_task = null;
	
	public Queue<FJTask> queue = null;
	
	/**
	 * Constructor.
	 * 
	 * For book keeping convenience, the worker knows what stage it is
	 * in.  This really only applies to multi-stage systems.
	 * 
	 * @param stage
	 */
	public FJWorker(int stage) {
		this.stage = stage;
	}
	
	/**
	 * Constructor for single-stage systems.  The stage will default to 0.
	 */
	public FJWorker() {
		this(0);
	}

}
