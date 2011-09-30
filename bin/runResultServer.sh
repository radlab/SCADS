#!/bin/bash

bin/sbt "scala-engine/run-main edu.berkeley.cs.scads.storage.ScalaEngine --clusterAddress zk://zoo1.millennium.berkeley.edu,zoo2.millennium.berkeley.edu,zoo3.millennium.berkeley.edu/home/$USER/deploylib/ --dbDir results"
