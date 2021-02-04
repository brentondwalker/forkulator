package forkulator.randomprocess;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * java -cp forkulator.jar forkulator.randomprocess.ExponentialOrderStatistics > output.dat
 * 
 * @author brenton
 *
 */
public class ExponentialOrderStatistics {

	protected Random rand = ThreadLocalRandom.current();
	
	int orderstat_N = 32;
	double rate = 1.0;
	double arrival_rate = 0.7;
	double pdf_min = 0.0;
	double pdf_max = 100.0;
	double binwidth = 0.01;
	int num_samples = 10000000;
	
	double[] exp_samples;
	double[] exp_histogram;
	
	double[][] exp_orderstat_samples;
	double[][] exp_orderstat_histogram;

	double[][] exp_orderstat_pdf;

	static double theta_min = 0.0;
	double theta_max = rate;
	double theta_incr = 0.01;
	double[][] exp_emp_orderstat_mgf;
	double[][] exp_anl_orderstat_mgf;
	double[][] exp_prod_orderstat_mgf;
	
    double[][] rho_a;
	double[][] rho_s;
	/**
	 * 
	 * @return
	 */
	public double sample() {
		return -Math.log(rand.nextDouble())/rate;
	}
	
	/**
	 * 
	 * @return
	 */
	public double[] orderstatSample() {
		double[] x = new double[orderstat_N];
		for (int i=0; i<orderstat_N; i++) {
			x[i] = sample();
		}
		Arrays.sort(x);
		return x;
	}
	
	
	/**
	 * 
	 */
	public void printExpData() {
		for (int i=0; i<exp_histogram.length; i++) {
			System.out.println(""+(i*binwidth)+"\t"+(exp_histogram[i]/num_samples/binwidth));
		}
	}

	/**
	 * 
	 */
	public void printExpOrderstatData() {
		for (int i=0; i<exp_orderstat_histogram[0].length; i++) {
			System.out.print(""+(i*binwidth));
			for (int j=0; j<orderstat_N; j++) {
				System.out.print("\t"+(exp_orderstat_histogram[j][i]/num_samples/binwidth));
			}
			System.out.println("");
		}
	}

	/**
	 * 
	 */
	public void printExpOrderstatPDF() {
		for (int i=0; i<exp_orderstat_pdf[0].length; i++) {
			System.out.print(""+(i*binwidth));
			for (int j=0; j<orderstat_N; j++) {
				System.out.print("\t"+(exp_orderstat_pdf[j][i]));
			}
			System.out.println("");
		}
	}

	/**
	 * 
	 */
	public void printExpEmpOrderstatMGF() {
		for (int i=0; i<exp_emp_orderstat_mgf[0].length; i++) {
			double theta = theta_min + i*theta_incr;
			System.out.print(""+theta);
			for (int k=0; k<orderstat_N; k++) {
				System.out.print("\t"+exp_emp_orderstat_mgf[k][i]);
			}
			System.out.println("");
		}
	}

	/**
	 * 
	 */
	public void printExpAnlOrderstatMGF() {
		for (int i=0; i<exp_anl_orderstat_mgf[0].length; i++) {
			double theta = theta_min + i*theta_incr;
			System.out.print(""+theta);
			for (int k=0; k<orderstat_N; k++) {
				System.out.print("\t"+exp_anl_orderstat_mgf[k][i]);
			}
			System.out.println("");
		}
	}

    /**
     * 
     */
    public void printExpProdOrderstatMGF() {
        for (int i=0; i<exp_prod_orderstat_mgf[0].length; i++) {
            double theta = theta_min + i*theta_incr;
            System.out.print(""+theta);
            for (int k=0; k<orderstat_N; k++) {
                System.out.print("\t"+exp_prod_orderstat_mgf[k][i]);
            }
            System.out.println("");
        }
    }

	/**
	 * 
	 */
	public void printRhos() {
		for (int i=0; i<rho_a[0].length; i++) {
			double theta = theta_min + i*theta_incr;
			System.out.print(""+theta+"\t"+rho_a[0][i]);
			for (int k=0; k<orderstat_N; k++) {
				System.out.print("\t"+rho_s[k][i]);
			}
			System.out.println("");
		}
		
	}
	
	/**
	 * 
	 * @param rate
	 * @param num_samples
	 */
	@SuppressWarnings("unused")
	public ExponentialOrderStatistics(double rate, int num_samples) {
		this.rate = rate;
		this.theta_max = rate;
		this.num_samples = num_samples;
		exp_samples = new double[num_samples];
		exp_histogram = new double[(int)Math.round(0.5 + (pdf_max-pdf_min)/binwidth)];
		
		for (int i=0; i<num_samples; i++) {
			exp_samples[i] = this.sample();
			if (exp_samples[i] >= pdf_min && exp_samples[i] <= pdf_max) {
				exp_histogram[(int)Math.round(exp_samples[i]/binwidth)]++;
			}
		}
		
		exp_orderstat_samples = new double[orderstat_N][num_samples];
		exp_orderstat_histogram = new double[orderstat_N][(int)Math.round(0.5 + (pdf_max-pdf_min)/binwidth)];
		for (int i=0; i<num_samples; i++) {
			double[] x = this.orderstatSample();
			for (int j=0; j<orderstat_N; j++) {
				exp_orderstat_samples[j][i] = x[j];
				if (x[j] >= pdf_min && x[j] <= pdf_max) {
					exp_orderstat_histogram[j][(int)Math.round(x[j]/binwidth)]++;
				}
			}
		}
		
		exp_orderstat_pdf = new double[orderstat_N][(int)Math.round(0.5 + (pdf_max-pdf_min)/binwidth)];
		for (int k=1; k<=exp_orderstat_pdf.length; k++) {
			for (int i=0; i<exp_orderstat_pdf[0].length; i++) {
				double x = i * binwidth;
				exp_orderstat_pdf[k-1][i] = exponentialOrderstatPDF(orderstat_N, k, x, rate);
			}
		}
		
		exp_emp_orderstat_mgf = new double[orderstat_N][(int)Math.round(0.5+ (theta_max-theta_min)/theta_incr)];
		exp_anl_orderstat_mgf = new double[orderstat_N][(int)Math.round(0.5+ (theta_max-theta_min)/theta_incr)];
        exp_prod_orderstat_mgf = new double[orderstat_N][(int)Math.round(0.5+ (theta_max-theta_min)/theta_incr)];
        /*
		for (int k=1; k<=orderstat_N; k++) {
			System.err.println("exponentialEmpericalOrderstatMGF k="+k);
			for (int i=0; i<exp_emp_orderstat_mgf[0].length; i++) {
				double theta = theta_min + i*theta_incr;
				//exp_emp_orderstat_mgf[k-1][i] = exponentialEmpericalOrderstatMGF(k, theta);
                exp_anl_orderstat_mgf[k-1][i] = this.exponentialAnalytialOrderstatMGF(k, theta);
                exp_prod_orderstat_mgf[k-1][i] = this.exponentialAnalytialProductOrderstatMGF(k, theta);
			}
		}
		*/
		
		rho_a = new double[orderstat_N][(int)Math.round(0.5 + (theta_max-theta_min)/theta_incr)];
		rho_s = new double[orderstat_N][(int)Math.round(0.5 + (theta_max-theta_min)/theta_incr)];
		
		for (int k=1; k<=orderstat_N; k++) {
			for (int i=0; i<rho_a[0].length; i++) {
				double theta = theta_min + i*theta_incr;
				if (theta > 0.0) {
					rho_a[k-1][i] = -(1.0/theta) * Math.log(arrival_rate/(arrival_rate + theta));
					double M = 1.0;
					for (int j=0; j<k; j++) {
						M *= (orderstat_N-j)*rate/((orderstat_N-j)*rate - theta);
					}
					rho_s[k-1][i] = (1.0/theta) * Math.log(M);
				} else {
					rho_a[k-1][i] = 0.0;
					rho_s[k-1][i] = 0.0;
				}
			}

			double mu = rate * k / orderstat_N;
			double thetaeps = 0.000001;
			double tt = findThetaLimit(orderstat_N, k, arrival_rate, mu, 0.0, mu, thetaeps);
			if (tt >= 0.0) {
				double ra = rhoA(arrival_rate, tt);
				double rs = rhoS(orderstat_N, k, mu, tt);
				//System.err.println("k="+k+"\t max theta="+tt+"\t rhoA="+ra+"\t rhoS="+rs+"\t diff="+(ra-rs));
			} else {
				//System.err.println("k="+k+"\t max theta="+tt);
			}
		}
		
		/*
		 * plot [0:1][0:500] 'sqlb_bound_A05_S10_N8.dat' using 3:8 w lp, 'sqlb_bound_A05_S10_N16.dat' using 3:8 w lp, 'sqlb_bound_A05_S10_N32.dat' using 3:8 w l, 'sqlb_bound_A05_S10_N64.dat' using 3:8 w l, 'sqlb_bound_A05_S10_N128.dat' using 3:8 w l
		 * plot [0:1][0:300] 'sqlb_bound_A03_S10_N2.dat' using 3:8 w lp,'sqlb_bound_A03_S10_N4.dat' using 3:8 w lp, 'sqlb_bound_A03_S10_N8.dat' using 3:8 w lp, 'sqlb_bound_A03_S10_N16.dat' using 3:8 w lp, 'sqlb_bound_A03_S10_N32.dat' using 3:8 w l, 'sqlb_bound_A03_S10_N64.dat' using 3:8 w l, 'sqlb_bound_A03_S10_N128.dat' using 3:8 w l
		 * plot [0:1][0:300] 'sqlb_bound_A07_S10_N2.dat' using 3:8 w lp,'sqlb_bound_A07_S10_N4.dat' using 3:8 w lp,'sqlb_bound_A07_S10_N8.dat' using 3:8 w lp, 'sqlb_bound_A07_S10_N16.dat' using 3:8 w lp, 'sqlb_bound_A07_S10_N32.dat' using 3:8 w l, 'sqlb_bound_A07_S10_N64.dat' using 3:8 w l, 'sqlb_bound_A07_S10_N128.dat' using 3:8 w l
		 */
		double[] sqlb_W_bound = new double[orderstat_N];
		double epsilon3 = 1e-3;
		double epsilon6 = 1e-6;
		int[] N_vals = { 1, 2, 4, 8, 16, 32, 64, 128 };
		for (int N : N_vals) {
			for (int k=1; k<N; k++) {
				double mu = rate * k / N;
				double thetaeps = 0.000001;
				//double tlimit = findThetaLimit(N, k, arrival_rate, mu, 0.0, rate, thetaeps);
				double tlimit = findThetaLimitUnbounded(N, k, arrival_rate, mu, 0.0, thetaeps);
				if (tlimit >= 0.0) {
					double tt = findOptimalTheta(N, k, arrival_rate, mu, theta_min, tlimit, thetaeps);
					double kdbn = (1.0*k)/N;
					double ra = rhoA(arrival_rate, tt);
					double rs = rhoS(N, k, mu, tt);  // rho_S = rho_Z
					double sa = 0.0;  // sigma_A = 0 for exponential arrivals
					double alpha = Math.exp(tt * sa) / (1.0 - Math.exp(-tt * (ra - rs)) );
					double W_bound_e3 = (-1.0/tt) * Math.log(epsilon3/alpha);
					double W_bound_e6 = (-1.0/tt) * Math.log(epsilon6/alpha);
					//sqlb_W_bound[k-1] = (-1.0/tt) * Math.log(epsilon/alpha);
					//System.err.println(""+N+"\t"+k+"\t"+kdbn+"\t"+arrival_rate+"\t"+mu+"\t"+(mu*N/k)+"\t"+tt+"\t"+W_bound_e3+"\t"+W_bound_e6);
				}
			}
			//System.err.println("");
		}
		
		// look at the integrands used to compute FT(tau)
		if (false) {
			int k = 8;
			int N = 16;
			double tau = 100;
			double mu = rate * k / N;
			double thetaeps = 0.000001;
			double tt = findThetaLimit(N, k, arrival_rate, mu, 0.0, mu, thetaeps);
			double ra = rhoA(arrival_rate, tt);
			double rs = rhoS(N, k, mu, tt);  // rho_S = rho_Z
			double sa = 0.0;  // sigma_A = 0 for exponential arrivals
			double alpha = Math.exp(tt * sa) / (1.0 - Math.exp(-tt * (ra - rs)) );
			for (double xx=0.0; xx<=tau; xx+=(tau/1000.0)) {
				double FWtmx = 1.0 - alpha * Math.exp(-tt*(tau-xx));
				double fQ = k * mu * Math.exp(-mu*xx) * Math.pow((1.0 - Math.exp(-mu*xx)), k-1);
				System.out.println(""+xx+"\t"+FWtmx+"\t"+fQ+"\t"+(FWtmx*fQ));
			}
		}

		// look at how W quantile bound varies with theta for a particular tau
		if (false) {
			int k = 32;
			int N = 128;
			double[] tau_list = { 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0 };
			double mu = rate * k / N;
			double thetaeps = 0.000001;
			//double tmax = findThetaLimit(N, k, arrival_rate, mu, 0.0, mu, thetaeps);
			double tmax = 1.0;
			double theta_incr = (tmax - theta_min)/1000;
			for (double tt=theta_min+theta_incr; tt<tmax; tt+= theta_incr) {
				double ra = rhoA(arrival_rate, tt);
				double rs = rhoS(N, k, mu, tt);  // rho_S = rho_Z
				double sa = 0.0;  // sigma_A = 0 for exponential arrivals
				double alpha = Math.exp(tt * sa) / (1.0 - Math.exp(-tt * (ra - rs)) );
				double W_bound_e3 = (-1.0/tt) * Math.log(epsilon3/alpha);
				double W_bound_e6 = (-1.0/tt) * Math.log(epsilon6/alpha);
				System.out.println(""+tt+"\t"+ra+"\t"+rs+"\t"+alpha+"\t"+W_bound_e3+"\t"+W_bound_e6);
			}
		}

		// look at how FT varies with theta for a particular tau
		if (true) {
			int k = 1;
			int N = 16;
			double[] tau_list = { 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0 };
			double mu = rate * k / N;
			double thetaeps = 0.000001;
			double tmax = findThetaLimit(N, k, arrival_rate, mu, 0.0, mu, thetaeps);
			System.err.println("theta_max = "+tmax);
			double theta_incr = (tmax - theta_min)/1000;
			for (double tt=theta_min; tt<tmax; tt+= theta_incr) {
				double ra = rhoA(arrival_rate, tt);
				double rs = rhoS(N, k, mu, tt);  // rho_S = rho_Z
				double sa = 0.0;  // sigma_A = 0 for exponential arrivals
				double alpha = Math.exp(tt * sa) / (1.0 - Math.exp(-tt * (ra - rs)) );
				System.out.print(""+tt+"\t"+ra+"\t"+rs+"\t"+alpha);
				for (double tau : tau_list) {
					double TCDF = computeTCDF(alpha, k, mu, tt, tau);
					System.out.print("\t"+TCDF);
				}
				System.out.println("");
			}
		}
		
		/*
		double taueps = 0.0001;
		for (double tau=0.0; tau<(1.0-taueps); tau+=taueps) {
			System.err.print(""+orderstat_N+"\t"+"\t"+arrival_rate+"\t"+tau);

			for (int k=1; k<orderstat_N/2; k++) {
				double mu = rate * k / orderstat_N;
				double tt = findThetaLimit(orderstat_N, k, arrival_rate, mu, 0.0, mu, 0.000001);
				if (tt >= 0.0) {
					double ra = rhoA(arrival_rate, tt);
					double rs = rhoS(orderstat_N, k, mu, tt);  // rho_S = rho_Z
					double sa = 0.0;  // sigma_A = 0 for exponential arrivals
					double alpha = Math.exp(tt * sa) / (1.0 - Math.exp(-tt * (ra - rs)) );
					double Tbound = computeTBound(alpha, k, mu, tt, tau);
					System.err.print("\t"+k+"\t"+Tbound);
				}
			}
			System.err.println("");
		}
		*/
	}

	public static double computeTBoundBad(double alpha, int k, double mu, double theta, double tau) {
		double FT = 0.0;
		for (int i=1; i<=k; i++) {
			FT += Math.pow(-1, i-1) * binomial(k-1, i-1) * ( (1.0 - Math.exp(-i*mu*tau))/(i*mu) + (1.0 - Math.exp((theta-i)*mu*tau))*alpha*Math.exp(-theta*tau)/(theta-i*mu) );
		}
		FT *= k*mu;
		return FT;
	}

	
	public static double computeTBoundBadBad(double alpha, int k, double mu, double theta, double tau) {
		double FT = 0.0;
		for (int i=1; i<=k; i++) {
			FT += Math.pow(-1, i) * binomial(k, i) * ( (1.0 - Math.exp(-(i*mu-theta)*tau))/(i*mu-theta) );
		}
		FT *= theta * alpha * Math.exp(-theta*tau);
		FT += (1-alpha)*Math.pow((1-Math.exp(-mu*tau)), k);
		return FT;
	}
	
	public static double computeTCDF(double alpha, int k, double mu, double theta, double tau) {
		double FT = 0.0;
		
		for (int i=0; i<k; i++) {
			FT += Math.pow(-1, i)*binomial(k-1,i)*(
					(1.0 - Math.exp(-(i+1)*mu*tau))/((i+1)*mu)
					 + (1.0 - Math.exp((theta-(i+1)*mu)*tau)) *alpha*Math.exp(-theta*tau)/(theta-(i+1)*mu));
		}
		FT *= k*mu;
		
		return FT;
	}


	/**
	 * 
	 * @param N
	 * @param k
	 * @param lambdafindThetaLimit
	 * @param mu
	 * @param theta_min
	 * @param theta_max
	 * @param tol
	 * @return
	 */
	public static double findThetaLimit(int N, int k, double lambda, double mu, double theta_min, double theta_max, double tol) {
		// now we understand what the max value for theta is
		theta_max = (N-k)*mu;
		
		// rho_A is a decreasing fuction of theta, and rho_S is increasing, so
		// start at the lowest value of theta to see if anything is feasible.
		if (rhoS(N, k, mu, theta_min+tol) > rhoA(lambda, theta_min+tol)) {
			return -1.0;
		}
		
		if (rhoS(N, k, mu, theta_max) <= rhoA(lambda, theta_max)) {
			return theta_max;
		}
		
		// There is at least a value of theta for which the system is stable,
		// but the system becomes unstable for some theta<theta_max.
		// Do a binary search for the max value of theta.
		double l_theta = theta_min;
		double r_theta = theta_max;
		
		double theta = (r_theta + l_theta)/2.0;
		while ((r_theta - l_theta) > tol) {
			if (rhoS(N, k, mu, theta) > rhoA(lambda, theta)) {
				r_theta = theta;
			} else {
				l_theta = theta;
			}
			theta = (r_theta + l_theta)/2.0;
		}
		
		return l_theta;
	}

	/**
	 * 
	 * @param N
	 * @param k
	 * @param lambdafindThetaLimit
	 * @param mu
	 * @param theta_min
	 * @param theta_max
	 * @param tol
	 * @return
	 */
	public static double findThetaLimitUnbounded(int N, int k, double lambda, double mu, double theta_min, double tol) {
		// rho_A is a decreasing fuction of theta, and rho_S is increasing, so
		// start at the lowest value of theta to see if anything is feasible.
		if (rhoS(N, k, mu, theta_min+tol) > rhoA(lambda, theta_min+tol)) {
			return -1.0;
		}

		// find some big theta that breaks things
		double max_jump = mu*10000;
		double theta_jump = tol;
		while (rhoS(N, k, mu, theta_min+theta_jump) < rhoA(lambda, theta_min+theta_jump)) {
			theta_jump *= 2;
			if (theta_jump > max_jump) {
				System.err.println("WARNING: theta_jump got too big: "+theta_jump);
				break;
			}
		}
		double theta_max = theta_min + theta_jump;
				
		// There is at least a value of theta for which the system is stable,
		// but the system becomes unstable for some theta<theta_max.
		// Do a binary search for the max value of theta.
		double l_theta = theta_min;
		double r_theta = theta_max;
		
		double theta = (r_theta + l_theta)/2.0;
		while ((r_theta - l_theta) > tol) {
			if (rhoS(N, k, mu, theta) > rhoA(lambda, theta)) {
				r_theta = theta;
			} else {
				l_theta = theta;
			}
			theta = (r_theta + l_theta)/2.0;
		}
		
		return l_theta;
	}

	public static double computeWBound(int N, int k, double lambda, double mu, double theta, double epsilon) {
		double ra = rhoA(lambda, theta);
		double rs = rhoS(N, k, mu, theta);  // rho_S = rho_Z
		double sa = 0.0;  // sigma_A = 0 for exponential arrivals
		double alpha = Math.exp(theta * sa) / (1.0 - Math.exp(-theta * (ra - rs)) );
		double W_bound = (-1.0/theta) * Math.log(epsilon/alpha);
		return W_bound;
	}
	
	public static double findOptimalTheta(int N, int k, double lambda, double mu, double theta_min, double theta_max, double tol) {
		double epsilon3 = 1e-3;
		
		double l_theta = theta_min + tol;
		double r_theta = theta_max;
		double theta = (r_theta + l_theta)/2.0;

		while ((r_theta - l_theta) > tol) {
			double b1 = computeWBound(N, k, lambda, mu, theta, epsilon3);
			double b2 = computeWBound(N, k, lambda, mu, theta+tol, epsilon3);
			if (b1 < b2) {
				// bound is increasing at this theta
				r_theta = theta;
			} else {
				l_theta = theta;
			}
			theta = (r_theta + l_theta)/2.0;
		}
		return theta;
	}
	
	public static double rhoA(double lambda, double theta) {
		if (theta == 0.0) return 0.0;
		return -(1.0/theta) * Math.log(lambda/(lambda + theta));
	}
	
	public static double rhoS(int N, int k, double mu, double theta) {
		if (theta == 0.0) return 0.0;
		double M = 1.0;
		for (int j=0; j<k; j++) {
			M *= (N-j)*mu/((N-j)*mu - theta);
		}
		return (1.0/theta) * Math.log(M);
	}

	/**
	 * 
	 * @param n
	 * @param k
	 * @return
	 */
	public static long binomial(int n, int k) {
        if (k>n-k)
            k=n-k;

        long b=1;
        for (int i=1, m=n; i<=k; i++, m--)
            b=b*m/i;
        return b;
    }
	
	/**
	 * 
	 * @param k
	 * @param theta
	 * @return
	 */
	private double exponentialAnalytialOrderstatMGF(int k, double theta) {
		double M = 0.0;
		int parity = 1;
		for (int i=0; i<k; i++) {
			M += parity * binomial(k-1,i) / (i + 1 - rate*theta);
			parity *= -1;
		}
		M *= k;
		return M;
	}

	/**
     * 
     * @param k
     * @param theta
     * @return
     */
    private double exponentialAnalytialProductOrderstatMGF(int k, double theta) {
        double M = 1.0;
        for (int j=0; j<k; j++) {
            M *= (orderstat_N-j)*rate/((orderstat_N-j)*rate - theta);
        }
        return M;
    }

	
	/**
	 * 
	 * @param k
	 * @param theta
	 * @return
	 */
	private double exponentialEmpericalOrderstatMGF(int k, double theta) {
		double M = 0.0;
		for (int i=0; i<exp_orderstat_samples[k-1].length; i++) {
			M += Math.exp(exp_orderstat_samples[k-1][i]*theta);
		}
		return M/(double)exp_orderstat_samples[k-1].length;
	}

	
	/**
	 * 
	 * @param x
	 * @param rate
	 * @return
	 */
	public static double exponentialPDF(double x, double rate) {
		if (x < 0) {
			return 0.0;
		}
		return rate * Math.exp(-x*rate);
	}
	
	/**
	 * 
	 * @param x
	 * @param rate
	 * @return
	 */
	public static double exponentialCDF(double x, double rate) {
		if (x < 0) {
			return 0.0;
		}
		return 1.0 - Math.exp(-x*rate);
	}
	
	/**
	 * 
	 * @param N
	 * @param k
	 * @param x
	 * @param rate
	 * @return
	 */
	public static double exponentialOrderstatPDF(int N, int k, double x, double rate) {
		return (factorial(N)/factorial(k-1)/factorial(N-k))
				* Math.pow(exponentialCDF(x,rate), k-1)
				* Math.pow(1.0-exponentialCDF(x,rate), N-k)
				* exponentialPDF(x,rate);
	}
	
	/**
	 * 
	 * @param n
	 * @return
	 */
	public static double factorial(int n) {
		double x = 1.0;
		for (int i=1; i<=n; i++) {
			x *= (double)i;
		}
		return x;
	}
	
	
	/**
	 * plot [0:7][0:14] 'eosN.dat' using 1:2 w l, 'eosN.dat' using 1:3 w l, 'eosN.dat' using 1:4 w l, 'eosN.dat' using 1:5 w l, 'eosN.dat' using 1:6 w l, 'eosN.dat' using 1:7 w l, 'eosN.dat' using 1:8 w l, 'eosN.dat' using 1:9 w l, 'eosN.dat' using 1:10 w l, 'eosN.dat' using 1:11 w l, 'eosN.dat' using 1:12 w l, 'eosN.dat' using 1:13 w l, 'eosN.dat' using 1:14 w l, 'eosN.dat' using 1:15 w l, 'eosN.dat' using 1:16 w l, 'eosN.dat' using 1:17 w l
	 * plot [0:7][0:14] 'eosNpdf.dat' using 1:2 w l, 'eosNpdf.dat' using 1:3 w l, 'eosNpdf.dat' using 1:4 w l, 'eosNpdf.dat' using 1:5 w l, 'eosNpdf.dat' using 1:6 w l, 'eosNpdf.dat' using 1:7 w l, 'eosNpdf.dat' using 1:8 w l, 'eosNpdf.dat' using 1:9 w l, 'eosNpdf.dat' using 1:10 w l, 'eosNpdf.dat' using 1:11 w l, 'eosNpdf.dat' using 1:12 w l, 'eosNpdf.dat' using 1:13 w l, 'eosNpdf.dat' using 1:14 w l, 'eosNpdf.dat' using 1:15 w l, 'eosNpdf.dat' using 1:16 w l, 'eosNpdf.dat' using 1:17 w l
	 * plot [0:1][0:16] 'eosNempMGF.dat' using 1:2 w l, 'eosNempMGF.dat' using 1:3 w l, 'eosNempMGF.dat' using 1:4 w l, 'eosNempMGF.dat' using 1:5 w l, 'eosNempMGF.dat' using 1:6 w l, 'eosNempMGF.dat' using 1:7 w l, 'eosNempMGF.dat' using 1:8 w l, 'eosNempMGF.dat' using 1:9 w l, 'eosNempMGF.dat' using 1:10 w l, 'eosNempMGF.dat' using 1:11 w l, 'eosNempMGF.dat' using 1:12 w l, 'eosNempMGF.dat' using 1:13 w l, 'eosNempMGF.dat' using 1:14 w l, 'eosNempMGF.dat' using 1:15 w l, 'eosNempMGF.dat' using 1:16 w l, 'eosNempMGF.dat' using 1:7 w l
	 * plot [0:1][0:16] 'eosNanlMGF.dat' using 1:2 w l, 'eosNanlMGF.dat' using 1:3 w l, 'eosNanlMGF.dat' using 1:4 w l, 'eosNanlMGF.dat' using 1:5 w l, 'eosNanlMGF.dat' using 1:6 w l, 'eosNanlMGF.dat' using 1:7 w l, 'eosNanlMGF.dat' using 1:8 w l, 'eosNanlMGF.dat' using 1:9 w l, 'eosNanlMGF.dat' using 1:10 w l, 'eosNanlMGF.dat' using 1:11 w l, 'eosNanlMGF.dat' using 1:12 w l, 'eosNanlMGF.dat' using 1:13 w l, 'eosNanlMGF.dat' using 1:14 w l, 'eosNanlMGF.dat' using 1:15 w l, 'eosNanlMGF.dat' using 1:16 w l, 'eosNanlMGF.dat' using 1:7 w l
	 * plot [0:1][0:16] 'eosNprodMGF.dat' using 1:2 w l, 'eosNprodMGF.dat' using 1:3 w l, 'eosNprodMGF.dat' using 1:4 w l, 'eosNprodMGF.dat' using 1:5 w l, 'eosNprodMGF.dat' using 1:6 w l, 'eosNprodMGF.dat' using 1:7 w l, 'eosNprodMGF.dat' using 1:8 w l, 'eosNprodMGF.dat' using 1:9 w l, 'eosNprodMGF.dat' using 1:10 w l, 'eosNprodMGF.dat' using 1:11 w l, 'eosNprodMGF.dat' using 1:12 w l, 'eosNprodMGF.dat' using 1:13 w l, 'eosNprodMGF.dat' using 1:14 w l, 'eosNprodMGF.dat' using 1:15 w l, 'eosNprodMGF.dat' using 1:16 w l, 'eosNprodMGF.dat' using 1:7 w l
	 * plot 'eosNrho_A05_S10.dat' using 1:2 w l, 'eosNrho_A05_S10.dat' using 1:3 w l, 'eosNrho_A05_S10.dat' using 1:4 w l, 'eosNrho_A05_S10.dat' using 1:5 w l, 'eosNrho_A05_S10.dat' using 1:6 w l, 'eosNrho_A05_S10.dat' using 1:7 w l, 'eosNrho_A05_S10.dat' using 1:8 w l, 'eosNrho_A05_S10.dat' using 1:9 w l, 'eosNrho_A05_S10.dat' using 1:10 w l, 'eosNrho_A05_S10.dat' using 1:11 w l, 'eosNrho_A05_S10.dat' using 1:12 w l, 'eosNrho_A05_S10.dat' using 1:13 w l, 'eosNrho_A05_S10.dat' using 1:14 w l, 'eosNrho_A05_S10.dat' using 1:15 w l, 'eosNrho_A05_S10.dat' using 1:16 w l, 'eosNrho_A05_S10.dat' using 1:17 w l, 'eosNrho_A05_S10.dat' using 1:18 w l
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		ExponentialOrderStatistics eos = new ExponentialOrderStatistics(1.0, 1000000);
		//eos.printExpOrderstatData();
		//eos.printExpOrderstatPDF();
		//eos.printExpEmpOrderstatMGF();
		//eos.printExpAnlOrderstatMGF();
		//eos.printExpProdOrderstatMGF();
		//eos.printRhos();
	}
}
