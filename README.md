# forkulator

Simulator for fork-join queueing systems.

Fork-join queueing systems are models for several kinds of real-world systems, such as multi-path routing and parallelized computing architectures.  The canonical model has a system with _k_ parallel servers and jobs arriving that are composed of _k_ tasks.  Upon arrival the _k_ tasks are queued at the _k_ servers.  A job can depart when each of its tasks has finished processing.

There are several variations on the model.  This package simulates many types of fork-join systems and has interchangeable arrival and service processes and leaky bucket process regulators.

## Build

  `ant`
  
## Run

We include a runner script, `forkulator.sh`, that saves you from having to type out all the JVM options.  Here is an example simulating a standard fork-join system with exponential arrival and service processes and _k=10_.  The results will be saved to files with names staring with "testrun".

  `./forkulator.sh -A x 0.5 -S x 1.0 -i 1 -n 1000 -o testrun -q w -t 10 -w 10`

### Options
```
 -A,--arrivalprocess <arg>     arrival process
 -h,--help                     print help message
 -i,--samplinginterval <arg>   samplig interval
 -n,--numjobs <arg>            number of jobs to run
 -o,--outfile <arg>            the base name of the output files
 -q,--queuetype <arg>          queue type code
 -S,--serviceprocess <arg>     service process
 -t,--numtasks <arg>           number of tasks per job
 -w,--numworkers <arg>         number of workers/servers
```

### Queue/System Types

* `-q w` standard "worker-queue" fork-join system, where each server has a separate queue of tasks assigned to it.
* `-q s` "single-queue" system, where there is a single queue of tasks and tasks are serviced from that queue as workers become available.
* `-q td` deterministic thinning without resequencing
* `-q tdr` deterministic thinning with resequencing
* `-q tr` random thinning without resequencing
* `-q trr` random thinning with resequencing
* `-q wkl<l_diff>` standard "worker-queue" _(k,l)_ system with *k-l=l_diff*
* `-q skl<l_diff>` "single-queue" _(k,l)_ system with *k-l=l_diff*
* `-q msw<h>` multi-stage worker-queue system with _h_ stages.  Each task has the same service time at each stage.
* `-q mswi<h>` multi-stage worker-queue system with _h_ stages.  The tasks' service times at each stage are iid.


### Arrival/Service Processes

#### Constant

```
-A c <rate>
```

#### Exponential

```
-A x <rate>
```

#### Erlang-_k_

```
-A e<k> <rate>
```

#### Gaussian

```
-A g <mu> <sigma>
```

#### Weibull

Given one parameter the process will be normalized to have expectation 1.0.
```
-A w <shape>
```

Or you can specify both the shape and scale parameters.
```
-A w <shape> <scale>
```


## Run Faster

This simulator can be run on a [Spark](http://spark.apache.org/) cluster (version less than 2.0).  Check out the `forkulator-sbt` branch.  This branch uses sbt instead of ant to build, and requires the sbt-assembly plugin.  This mode of running is new and not yet well tested or documented.

```
git checkout forkulator-sbt
sbt assembly
```

The resulting jar will be put in `target/scala-2.10/forkulator-assembly-1.0.jar`.

This version adds the `-s` option to specify the number of slices to divide the simulation into.  An example of using spark-submit to run the simulation, generating 1,000,000 samples at intervals of 100 iterations, with _k=100_, divided into 500 slices:
```
./bin/spark-submit --master spark://1.2.3.4:7077  \
                   --executor-memory 40g \
                   --class forkulator.FJSparkSimulator \
                   <path-to-forkulator>/target/scala-2.10/forkulator-assembly-1.0.jar \
                   -q w -A x 0.5 -S x 1.0 -w 100 -t 100 -i 100 -n 1000000 -o testrun -s 500
```

