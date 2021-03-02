package forkulator.randomprocess;

public class SumTwoIntertimeProcesses extends IntertimeProcess {

	public IntertimeProcess first_process;
	public IntertimeProcess second_process;

	public SumTwoIntertimeProcesses(IntertimeProcess first_process, IntertimeProcess second_process) {
		this.first_process = first_process;
		this.second_process = second_process;
	}
	
	@Override
	public double nextInterval(int jobSize) {
		return first_process.nextInterval(jobSize) + second_process.nextInterval(jobSize);
	}

	@Override
	public double nextInterval(double time) {
		return first_process.nextInterval(1) + second_process.nextInterval(1);
	}

	@Override
	public IntertimeProcess clone() {
		return new SumTwoIntertimeProcesses(first_process, second_process);
	}

	@Override
	public String processParameters() {
		return ""+this.first_process.processParameters() + " " + second_process.processParameters();
	}

}
