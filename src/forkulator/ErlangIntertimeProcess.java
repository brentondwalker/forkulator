package forkulator;

public class ErlangIntertimeProcess extends IntertimeProcess {

	public double rate = 1.0;
	public int k = 1;
	
	public ErlangIntertimeProcess(double rate, int k) {
		this.rate = rate;
		this.k = k;
	}
	
	@Override
	public double nextInterval(double jobSize) {
		double p = 1.0;
		for (int i=0; i<k; i++) {
			p *= rand.nextDouble();
		}
		
		return -Math.log(p)/rate;
	}

}
