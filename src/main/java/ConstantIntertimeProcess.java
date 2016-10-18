package forkulator;

public class ConstantIntertimeProcess  extends IntertimeProcess {

	public double rate = 1.0;
	
	public ConstantIntertimeProcess(double rate) {
		this.rate = rate;
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return 1.0/rate;
	}

	@Override
	public double nextInterval(double time) {
		return nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new ConstantIntertimeProcess(rate);
	}

	@Override
	public String processParameters() {
		return ""+this.rate;
	}

}
