package forkulator.randomprocess;

import java.util.Iterator;
import java.util.List;

public class RerunIntertimeProcess extends IntertimeProcess {
	public List<Double> timeList;
	Iterator<Double> iterator = null;

	public RerunIntertimeProcess(List<Double> timeList) {
		this.timeList = timeList;
		iterator = timeList.iterator();
	}
	
	@Override
	public double nextInterval(int jobSize) {
		if (iterator.hasNext())
			return iterator.next();
		else {
			System.out.println("Requesting interval after all values already rerun.");
			return 0.0;
		}
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}
	
	@Override
	public IntertimeProcess clone() {
		return new RerunIntertimeProcess(timeList);
	}

	@Override
	public String processParameters() {
		return ""+this.timeList;
	}

	public int getSampleNum() {
		return this.timeList.size();
	}

}
