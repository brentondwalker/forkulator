#!/bin/bash

# this script effectively spoofs the "combined" fj/multiserver system by simulating a single b-wide channel of it

k=2
a=1
b=2
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q w -A e$a $2 -S e$a 1.0 -w 1 -t 1 -n 1000000000 -i 100 -o testdata/combinedmulti_$1q_l$3_mu10_a$a_b$b >> testdata/combinedmulti_$1q_l$3_mu10_means.dat &
k=4
a=2
b=2
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q w -A e$a $2 -S e$a 1.0 -w 1 -t 1 -n 1000000000 -i 100 -o testdata/combinedmulti_$1q_l$3_mu10_a$a_b$b >> testdata/combinedmulti_$1q_l$3_mu10_means.dat &
k=16
a=4
b=4
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q w -A e$a $2 -S e$a 1.0 -w 1 -t 1 -n 1000000000 -i 100 -o testdata/combinedmulti_$1q_l$3_mu10_a$a_b$b >> testdata/combinedmulti_$1q_l$3_mu10_means.dat &
k=256
a=8
b=8
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q w -A e$a $2 -S e$a 1.0 -w 1 -t 1 -n 1000000000 -i 100 -o testdata/combinedmulti_$1q_l$3_mu10_a$a_b$b >> testdata/combinedmulti_$1q_l$3_mu10_means.dat 

for job in `jobs -p`
do
echo $job
    wait $job || let "FAIL+=1"
done

