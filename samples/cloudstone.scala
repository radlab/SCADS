/**
 * Sample script for deploying Cloudstone.
 *
 * You can run this script using:
 * mvn scala:script -DscriptFile=samples/cloudstone.scala
 */

import deploylib._
import deploylib.chef._
import deploylib.ec2._
import deploylib.physical._

import java.io.File

Chef.repoPath = "/Users/aaron/radlab/repo.tar.gz"

val amiIds = Map(
  "32-bit" -> "ami-e7a2448e",
  "64-bit" -> "ami-e4a2448d"
)

/******************************
// Request remote machines to deploy Cloudstone on.
val instances = Map(
  "webserver" -> EC2.runInstances(amiIds("64-bit"), 1, 1, EC2.keyName, "c1.xlarge", "us-east-1a"),
  "workload"  -> EC2.runInstances(amiIds("64-bit"), 1, 1, EC2.keyName, "c1.xlarge", "us-east-1a")
)

// Wait until all remote machines are running.
for (instance <- instances.values) {
  instance.waitUntilRunning()
}
******************************/

var instances = Map(
  "webserver" -> new PhysicalInstance("ec2-204-236-240-24.compute-1.amazonaws.com", "root", new File("/Users/aaron/.ec2/id_rsa-abeitch")),
  "workload"  -> new PhysicalInstance("ec2-75-101-181-190.compute-1.amazonaws.com", "root", new File("/Users/aaron/.ec2/id_rsa-abeitch"))
)

// Create the service configurations.
var configs: Map[String,Map[String,Any]] = Map(
  "rails" -> Map(
    "log_level" -> "debug",
    "ports" -> Map(
      "count" -> 16,
      "start" -> 3000
    )
  ),
  "mysql" -> Map(
    // Use defaults.
  ),
  "haproxy" -> Map(
    // Use defaults.
  ),
  "nginx" -> Map(
    // Use defaults.
  ),
  "faban" -> Map(
    "debug" -> false
  )
)

// Create the services.
var services = Map(
  "rails"   -> new RailsService(instances("webserver"), configs("rails")),
  "mysql"   -> new MySQLService(instances("webserver"), configs("mysql")),
  "haproxy" -> new HAProxyService(instances("webserver"), configs("haproxy")),
  "nginx"   -> new NginxService(instances("webserver"), configs("nginx")),
  "faban"   -> new FabanService(instances("workload"), configs("faban"))
)

// Configure the service dependencies.
services("rails").addDependency(services("mysql"))
services("rails").addDependency(services("haproxy"))
services("rails").addDependency(services("faban"))
services("haproxy").addDependency(services("rails"))
services("nginx").addDependency(services("haproxy"))
services("faban").addDependency(services("mysql"))
services("faban").addDependency(services("nginx"))

// Start the services.
for (service <- services.values) {
  println(service.recipeName)
  service.start
}
