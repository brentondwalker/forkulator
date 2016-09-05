#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 1 1 $2 1.0 100000000 100 testdata/multiserver_$1_l$2_mu10_w1 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 2 2 $2 1.0 100000000 100 testdata/multiserver_$1_sq_l$2_mu10_w2 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 4 4 $2 1.0 100000000 100 testdata/multiserver_$1_sq_l$2_mu10_w4 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 8 8 $2 1.0 100000000 100 testdata/multiserver_$1_sq_l$2_mu10_w8 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 16 $2 1.0 10000000 100 testdata/multiserver_$1_sq_l$2_mu10_w16 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 32 32 $2 1.0 10000000 100 testdata/multiserver_$1_sq_l$2_mu10_w32 >> testdata/multiserver_$1_l$2_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 64 64 $2 1.0 10000000 100 testdata/multiserver_$1_sq_l$2_mu10_w64 >> testdata/multiserver_$1_l$2_means.dat

