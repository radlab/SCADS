#!/bin/bash

exec /usr/lib/jvm/java-6-sun/bin/java -Djava.library.path=/work/mesos-production/lib/java -cp /work/marmbrus/mesos/log4j.properties:/work/marmbrus/mesos/mesos-scads-2.1.0-SNAPSHOT-jar-with-dependencies.jar edu.berkeley.cs.scads.mesos.JavaExecutor $@
