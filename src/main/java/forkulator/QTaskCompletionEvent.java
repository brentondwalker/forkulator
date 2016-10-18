package forkulator;

public class QTaskCompletionEvent extends QEvent {
	
	public FJTask task = null;
	
	public QTaskCompletionEvent(FJTask task, double time) {
		this.time = time;
		this.task = task;
	}

}
