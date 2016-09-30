#!/bin/bash

java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"1" -A x $2 -S x 1.0 -w $4 -t $4 -n 500000000 -i 100 -o new-fjpaper-data/fj_$1"1"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat &
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"2" -A x $2 -S x 1.0 -w $4 -t $4 -n 500000000 -i 100 -o new-fjpaper-data/fj_$1"2"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat &
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"16" -A x $2 -S x 1.0 -w $4 -t $4 -n 100000000 -i 100 -o new-fjpaper-data/fj_$1"16"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat &
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"32" -A x $2 -S x 1.0 -w $4 -t $4 -n 50000000 -i 100 -o new-fjpaper-data/fj_$1"32"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat &
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"64" -A x $2 -S x 1.0 -w $4 -t $4 -n 1000000 -i 100 -o new-fjpaper-data/fj_$1"64"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat &
java -Xmx5g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.3.1.jar" forkulator.FJSimulator -q $1"96" -A x $2 -S x 1.0 -w $4 -t $4 -n 1000000 -i 100 -o new-fjpaper-data/fj_$1"96"q_l$3_mu10_w$4t$4 >> new-fjpaper-data/fj_$1q_l$3_w$4t$4_means.dat


