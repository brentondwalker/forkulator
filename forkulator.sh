#!/bin/bash
java -Xmx15g -cp "bin:lib/commons-math3-3.6.1.jar:lib/commons-cli-1.2.jar" forkulator.FJSimulator "$@"
