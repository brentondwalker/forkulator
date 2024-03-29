package forkulator.randomprocess;

import java.util.Vector;

/**
 * HarmonicNumber class
 * 
 * Harmonic Numbers are simple to compute.  This class is mainly intended to compute
 * stability conditions for multi-server barrier systems with start and end barriers.
 * This is effectively what I have been calling "narrow Split-Merge".  The question is
 * whether this is better or worse than equivalent standard Split-Merge where
 * num_tasks=num_workers.  On one hand jobs can overtake each other, which limits the
 * ability of stragglers to block the whole system, but when the num_tasks does not
 * divide num_workers, it guarantees some workers will be
 * 
 * @author brenton
 *
 */
public class HarmonicNumber {

	/**
	 * Compute the harmonic number
	 * 
	 * @param n
	 * @return
	 */
	public static double Hn(int n) {
		double s = 0.0;
		for (int k=1; k<=n; k++) {
			s += 1.0/k;
			//System.err.println("k="+k+"\ts="+s);
		}
		return s;
	}
	
	/**
	 * For the (k/s) vs T plots we make for BEM systems, we would like to compute
	 * the maximum number of tasks/job where the system is stable (or the minimum
	 * where it becomes unstable).  This should return the max k for which the
	 * system is stable, or 0 if it is never stable.
	 * 
	 * @param num_workers
	 * @param lambda
	 * @param mu
	 * @return
	 */
	public static int kStability(int num_workers, double lambda, double mu) {
		int k = 0;
		double Hk = 0;
		
		for (k=1; k<=num_workers; k++) {
			Hk += 1.0/k;
			double mu_scaled = mu*k/num_workers;
			
			if (k > (num_workers*mu_scaled)/(lambda*Hk)) {
				return k-1;
			}
		}
		
		return num_workers;
	}

	
	/**
	 * For the (k/s) vs T plots we make for BEM systems, we would like to compute
	 * the maximum number of tasks/job where the system is stable (or the minimum
	 * where it becomes unstable).  This should return the max k for which the
	 * system is stable, or 0 if it is never stable.
	 * 
	 * In the case of systems with start and end barriers, the BEM system is essentially
	 * a M|G|m server with m=floor(k/s).  If k|s, then the other stability test routine
	 * is valid, but otherwise we have some discretization issues.  To address that here
	 * we have to mess with the effective number of workers to round it down to the
	 * largest multiple of k less than or equal to num_workers.
	 * 
	 * This is Markus's solution for a two-barrier system.
	 * 
	 * @param num_workers
	 * @param lambda
	 * @param mu
	 * @return
	 */
	public static Vector<Integer> kStabilityDiscrete(int num_workers, double lambda, double mu) {
		Vector<Integer> stable_k = new Vector<Integer>();
		int k = 0;
		double Hk = 0;
		
		for (k=1; k<=num_workers; k++) {
			Hk += 1.0/k;
			double mu_scaled = mu*k/num_workers;
			int effective_num_workers = k * (int)Math.floor(((double)num_workers)/k);
			
			if (k < (effective_num_workers*mu_scaled)/(lambda*Hk)) {
				stable_k.add(k);
			}
		}
		
		return stable_k;
	}

	
	/**
	 * Similar to above.  This is Markus's solution to the BEM single-barrier system.
	 * 
	 * TODO: fix this
	 * 
	 * @param num_workers
	 * @param lambda
	 * @param mu
	 * @return
	 */
	public static Vector<Integer> kStabilityDiscreteBEM(int num_workers, double lambda, double mu) {
	    Vector<Integer> stable_k = new Vector<Integer>();
	    int k = 0;
	    double Hk = 0;

	    for (k=1; k<=num_workers; k++) {
	        Hk += 1.0/k;
	        double mu_scaled = mu*k/num_workers;
	        int effective_num_workers = k * (int)Math.floor(((double)num_workers)/k);

	        if (k < (effective_num_workers*mu_scaled)/(lambda*Hk)) {
	            stable_k.add(k);
	        }
	    }

	    return stable_k;
	}

	
	
	public static void maxStableRho1Barrier(int tasks_per_job, int max_workers) {
	    //for (int s=tasks_per_job; s<=max_workers; s+=tasks_per_job) {
	    double Hk = Hn(tasks_per_job);
	    //System.err.println("Hk = "+Hk+"\ttasks_per_job="+tasks_per_job);
	    double max_2b_rho = 1.0/Hk;
	    
	    for (int s=tasks_per_job; s<=max_workers; s++) {
	        double sum = 0.0;
	        for (int j=0; j<tasks_per_job; j++) {
	            sum += 1.0/(tasks_per_job - j*tasks_per_job/(1.0*s));
	        }
	        double rho_max = 1.0/sum;
	        System.out.println(""+tasks_per_job+"\t"+s+"\t"+rho_max+"\t"+max_2b_rho);
	    }
	}
	

	/**
	 * print usage message
	 */
	public static void usage() {
		System.out.println("usage: HarmonicNumber <num_workers> <lambda> <mu>");
	}
	
	
	/**
	 * main()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		//if (args.length != 3) {
		//	usage();
		//	System.exit(0);
		//}
		
		//int num_workers = Integer.parseInt(args[0]);
		//double lambda = Double.parseDouble(args[1]);
		//double mu = Double.parseDouble(args[2]);
		//int k_stable = kStability(num_workers, lambda, mu);
		//System.out.println("kStability\t"+num_workers+"\t"+lambda+"\t"+mu+"\t"+k_stable+"\t"+((double)k_stable/num_workers));
		//Vector<Integer> k_stable_list = kStabilityDiscrete(num_workers, lambda, mu);
		//for (int kk : k_stable_list) {
		//	System.out.println(kk);
		//}
		int tasks_per_job = Integer.parseInt(args[0]);
		int max_workers = Integer.parseInt(args[1]);
		maxStableRho1Barrier(tasks_per_job, max_workers);
	}
	
}
