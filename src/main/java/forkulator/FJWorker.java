package forkulator;

import java.util.Queue;

public class FJWorker {
		
	public int stage = 0;
	
	public FJTask current_task = null;
	
	public Queue<FJTask> queue = null;
	
	public FJWorker(int stage) {
		this.stage = stage;
	}
	
}
