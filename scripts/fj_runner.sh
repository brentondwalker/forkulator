#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 1 -t 1 -n 100000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w1t1 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 2 -t 2 -n 100000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w2t2 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 4 -t 4 -n 100000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w4t4 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 8 -t 8 -n 100000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w8t8 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 16 -t 16 -n 100000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w16t16 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 32 -t 32 -n 10000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w32t32 >> testdata/fj_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S x 1.0 -w 64 -t 64 -n 10000000 -i 100 -o testdata/fj_$1q_l$3_mu10_w64t64 >> testdata/fj_$1q_l$3_means.dat

