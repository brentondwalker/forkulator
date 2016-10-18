package forkulator;

public class QTaskCompletionEvent extends QEvent {
	
	public FJTask task = null;
	
	public QTaskCompletionEvent(FJTask task, double time) {
		this.time = time;
		this.task = task;
	}
	
	@Override
	public void happen() {
		// TODO Auto-generated method stub

	}

}
