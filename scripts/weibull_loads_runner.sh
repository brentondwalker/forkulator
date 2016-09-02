#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.0001 $1 1000000000 100 testdata/weibull_loads_sq_l00001_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.001 $1 1000000000 100 testdata/weibull_loads_sq_l0001_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.005 $1 1000000000 100 testdata/weibull_loads_sq_l005_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.01 $1 1000000000 100 testdata/weibull_loads_sq_l001_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.05 $1 1000000000 100 testdata/weibull_loads_sq_l005_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.1 $1 1000000000 100 testdata/weibull_loads_sq_l01_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.2 $1 1000000000 100 testdata/weibull_loads_sq_l02_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.3 $1 1000000000 100 testdata/weibull_loads_sq_l03_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.4 $1 1000000000 100 testdata/weibull_loads_sq_l04_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.5 $1 1000000000 100 testdata/weibull_loads_sq_l05_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.6 $1 1000000000 100 testdata/weibull_loads_sq_l06_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.7 $1 1000000000 100 testdata/weibull_loads_sq_l07_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.8 $1 1000000000 100 testdata/weibull_loads_sq_l08_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.9 $1 1000000000 100 testdata/weibull_loads_sq_l09_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.95 $1 1000000000 100 testdata/weibull_loads_sq_l095_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.97 $1 1000000000 100 testdata/weibull_loads_sq_l097_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator s 16 16 0.98 $1 1000000000 100 testdata/weibull_loads_sq_l098_shape$2_w16t16 >> testdata/weibull_loads_sq_shape$2_w16t16_means.dat

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.0001 $1 1000000000 100 testdata/weibull_loads_wq_l00001_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.001 $1 1000000000 100 testdata/weibull_loads_wq_l0001_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.005 $1 1000000000 100 testdata/weibull_loads_wq_l005_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.01 $1 1000000000 100 testdata/weibull_loads_wq_l001_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.05 $1 1000000000 100 testdata/weibull_loads_wq_l005_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.1 $1 1000000000 100 testdata/weibull_loads_wq_l01_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.2 $1 1000000000 100 testdata/weibull_loads_wq_l02_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.3 $1 1000000000 100 testdata/weibull_loads_wq_l03_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.4 $1 1000000000 100 testdata/weibull_loads_wq_l04_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.5 $1 1000000000 100 testdata/weibull_loads_wq_l05_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.6 $1 1000000000 100 testdata/weibull_loads_wq_l06_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.7 $1 1000000000 100 testdata/weibull_loads_wq_l07_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.8 $1 1000000000 100 testdata/weibull_loads_wq_l08_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.9 $1 1000000000 100 testdata/weibull_loads_wq_l09_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.95 $1 1000000000 100 testdata/weibull_loads_wq_l095_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.97 $1 1000000000 100 testdata/weibull_loads_wq_l097_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator w 16 16 0.98 $1 1000000000 100 testdata/weibull_loads_wq_l098_shape$2_w16t16 >> testdata/weibull_loads_wq_shape$2_w16t16_means.dat


