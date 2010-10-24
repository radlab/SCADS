import deploylib._
import deploylib.mesos._
import deploylib.ec2._
import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.piql._
import edu.berkeley.cs.avro.marker._
import edu.berkeley.cs.avro.runtime._
import java.io.File

import edu.berkeley.cs.scads.perf._
import scadr.cardinality.CardinalityExperiment
import scadr.scale.ScadrScaleExperiment

implicit val scheduler = LocalExperimentScheduler(System.getProperty("user.name") + " console", "1@10.123.6.101:5050")
implicit def classpath = Deploy.s3Classpath
lazy val ec2zoo = ZooKeeperNode("zk://mesos-ec2/")
