#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.0001 $2 1000000000 100 testdata/weibull_loads_$1q_l00001_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.001 $2 1000000000 100 testdata/weibull_loads_$1q_l0001_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.005 $2 1000000000 100 testdata/weibull_loads_$1q_l005_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.01 $2 1000000000 100 testdata/weibull_loads_$1q_l001_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.05 $2 1000000000 100 testdata/weibull_loads_$1q_l005_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.1 $2 1000000000 100 testdata/weibull_loads_$1q_l01_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.2 $2 1000000000 100 testdata/weibull_loads_$1q_l02_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.3 $2 1000000000 100 testdata/weibull_loads_$1q_l03_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.4 $2 1000000000 100 testdata/weibull_loads_$1q_l04_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.5 $2 1000000000 100 testdata/weibull_loads_$1q_l05_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.6 $2 1000000000 100 testdata/weibull_loads_$1q_l06_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.7 $2 1000000000 100 testdata/weibull_loads_$1q_l07_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.8 $2 1000000000 100 testdata/weibull_loads_$1q_l08_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.9 $2 1000000000 100 testdata/weibull_loads_$1q_l09_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.95 $2 1000000000 100 testdata/weibull_loads_$1q_l095_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.97 $2 1000000000 100 testdata/weibull_loads_$1q_l097_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 0.98 $2 1000000000 100 testdata/weibull_loads_$1q_l098_shape$3_w16t16 >> testdata/weibull_loads_$1q_shape$3_w16t16_means.dat

