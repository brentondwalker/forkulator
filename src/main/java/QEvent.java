package forkulator;

import java.util.LinkedList;

public abstract class QEvent {

	public static LinkedList<QEvent> eventQueue = null;

	public double time = 0.0;
	
	public static void setEventQueue(LinkedList<QEvent> eventQueue) {
		QEvent.eventQueue = eventQueue;
	}
	
	public abstract void happen();
	
}
