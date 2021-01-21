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
	
	int orderstat_N = 16;
	double rate = 1.0;
	double pdf_min = 0.0;
	double pdf_max = 100.0;
	double binwidth = 0.01;
	int num_samples = 10000000;
	
	double[] exp_samples;
	double[] exp_histogram;
	
	double[][] exp_orderstat_samples;
	double[][] exp_orderstat_histogram;

	double[][] exp_orderstat_pdf;

	double theta_min = 0.0;
	double theta_max = rate;
	double theta_incr = 0.01;
	double[][] exp_emp_orderstat_mgf;
	double[][] exp_anl_orderstat_mgf;
	double[][] exp_prod_orderstat_mgf;
	
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
	 * @param rate
	 * @param num_samples
	 */
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
		for (int k=1; k<=orderstat_N; k++) {
			System.err.println("exponentialEmpericalOrderstatMGF k="+k);
			for (int i=0; i<exp_emp_orderstat_mgf[0].length; i++) {
				double theta = theta_min + i*theta_incr;
				//exp_emp_orderstat_mgf[k-1][i] = exponentialEmpericalOrderstatMGF(k, theta);
                exp_anl_orderstat_mgf[k-1][i] = this.exponentialAnalytialOrderstatMGF(k, theta);
                exp_prod_orderstat_mgf[k-1][i] = this.exponentialAnalytialProductOrderstatMGF(k, theta);
			}
		}
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
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		ExponentialOrderStatistics eos = new ExponentialOrderStatistics(1.0, 1000000);
		//eos.printExpOrderstatData();
		//eos.printExpOrderstatPDF();
		//eos.printExpEmpOrderstatMGF();
		//eos.printExpAnlOrderstatMGF();
		eos.printExpProdOrderstatMGF();
	}
}
