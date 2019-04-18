package forkulator.helpers;

//This is a copy of from {@see <a href="https://github.com/apache/commons-math">common-maths</a>}


import org.apache.commons.math3.special.Gamma;
import org.apache.commons.math3.util.FastMath;

final class SaddlePointExpansion {
    private static final double HALF_LOG_2_PI = 0.5D * FastMath.log(6.283185307179586D);
    private static final double[] EXACT_STIRLING_ERRORS = new double[]{0.0D, 0.15342640972002736D, 0.08106146679532726D, 0.05481412105191765D, 0.0413406959554093D, 0.03316287351993629D, 0.02767792568499834D, 0.023746163656297496D, 0.020790672103765093D, 0.018488450532673187D, 0.016644691189821193D, 0.015134973221917378D, 0.013876128823070748D, 0.012810465242920227D, 0.01189670994589177D, 0.011104559758206917D, 0.010411265261972096D, 0.009799416126158804D, 0.009255462182712733D, 0.008768700134139386D, 0.00833056343336287D, 0.00793411456431402D, 0.007573675487951841D, 0.007244554301320383D, 0.00694284010720953D, 0.006665247032707682D, 0.006408994188004207D, 0.006171712263039458D, 0.0059513701127588475D, 0.0057462165130101155D, 0.005554733551962801D};

    private SaddlePointExpansion() {
    }

    static double getStirlingError(double z) {
        double ret;
        double z2;
        if (z < 15.0D) {
            z2 = 2.0D * z;
            if (FastMath.floor(z2) == z2) {
                ret = EXACT_STIRLING_ERRORS[(int)z2];
            } else {
                ret = Gamma.logGamma(z + 1.0D) - (z + 0.5D) * FastMath.log(z) + z - HALF_LOG_2_PI;
            }
        } else {
            z2 = z * z;
            ret = (0.08333333333333333D - (0.002777777777777778D - (7.936507936507937E-4D - (5.952380952380953E-4D - 8.417508417508417E-4D / z2) / z2) / z2) / z2) / z;
        }

        return ret;
    }

    static double getDeviancePart(double x, double mu) {
        double ret;
        if (FastMath.abs(x - mu) < 0.1D * (x + mu)) {
            double d = x - mu;
            double v = d / (x + mu);
            double s1 = v * d;
            double s = 0.0D / 0.0;
            double ej = 2.0D * x * v;
            v *= v;

            for(int j = 1; s1 != s; ++j) {
                s = s1;
                ej *= v;
                s1 += ej / (double)(j * 2 + 1);
            }

            ret = s1;
        } else {
            ret = x * FastMath.log(x / mu) + mu - x;
        }

        return ret;
    }

    static double logBinomialProbability(int x, int n, double p, double q) {
        double ret;
        if (x == 0) {
            if (p < 0.1D) {
                ret = -getDeviancePart((double)n, (double)n * q) - (double)n * p;
            } else {
                ret = (double)n * FastMath.log(q);
            }
        } else if (x == n) {
            if (q < 0.1D) {
                ret = -getDeviancePart((double)n, (double)n * p) - (double)n * q;
            } else {
                ret = (double)n * FastMath.log(p);
            }
        } else {
            ret = getStirlingError((double)n) - getStirlingError((double)x) - getStirlingError((double)(n - x)) - getDeviancePart((double)x, (double)n * p) - getDeviancePart((double)(n - x), (double)n * q);
            double f = 6.283185307179586D * (double)x * (double)(n - x) / (double)n;
            ret += -0.5D * FastMath.log(f);
        }

        return ret;
    }
}
