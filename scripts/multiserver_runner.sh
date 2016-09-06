#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 1 -t 1 -n 100000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w1 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 2 -t 1 -n 100000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w2 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 4 -t 1 -n 100000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w4 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 8 -t 1 -n 100000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w8 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 16 -t 1 -n 100000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w16 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 32 -t 1 -n 10000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w32 >> testdata/multiserver_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 64 -t 1 -n 10000000 -i 100 -o testdata/multiserver_$1q_l$3_mu10_w64 >> testdata/multiserver_$1q_l$3_means.dat

#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 1 1 $2 1.0 1000000000 100 testdata/multiserver_sq_l$3_mu10_w1 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 2 1 $2 2.0 1000000000 100 testdata/multiserver_sq_l$3_mu20_w2 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 4 1 $2 4.0 1000000000 100 testdata/multiserver_sq_l$3_mu40_w4 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 8 1 $2 8.0 1000000000 100 testdata/multiserver_sq_l$3_mu80_w8 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 16 1 $2 16.0 1000000000 100 testdata/multiserver_sq_l$3_mu160_w16 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 32 1 $2 32.0 1000000000 100 testdata/multiserver_sq_l$3_mu320_w32 >> testdata/multiserver_sq_l$3_means.dat
#java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar" forkulator.FJSimulator $1 64 1 $2 64.0 1000000000 100 testdata/multiserver_sq_l$3_mu640_w64 >> testdata/multiserver_sq_l$3_means.dat

