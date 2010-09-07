#!/bin/bash

mvn assembly:assembly -DskipTests
rsync -av target/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar r12:/work/marmbrus/mesos
