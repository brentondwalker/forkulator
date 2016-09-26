#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e1 1.0 -w 1 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w1 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e2 1.0 -w 2 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w2 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e4 1.0 -w 4 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w4 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e8 1.0 -w 8 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w8 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e16 1.0 -w 16 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w16 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e32 1.0 -w 32 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w32 >> testdata/multiserver_erlang_$1q_l$3_means.dat
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1 -A x $2 -S e64 1.0 -w 64 -t 1 -n 1000000000 -i 100 -o testdata/multiserver_erlang_$1q_l$3_mu10_w64 >> testdata/multiserver_erlang_$1q_l$3_means.dat


