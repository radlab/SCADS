/**
 * Sample script for deploying Cloudstone.
 *
 * You can run this script using:
 * mvn scala:script -DscriptFile=samples/loCal.scala
 */
 
import deploylib._
import deploylib.chef._
import deploylib.ec2._
import deploylib.physical._

import java.io.File

// Local path
Chef.repoPath = "/home/user/repo.tar.gz"

var instances = Map(
  "webserver_rails1" -> new PhysicalInstance("192.168.0.27", "root", new File("/home/user/.ssh/atom_key")),
  "rails2" -> new PhysicalInstance("192.168.0.28", "root", new File("/home/user/.ssh/atom_key")),
  "mysql" -> new PhysicalInstance("192.168.0.29", "root", new File("/home/user/.ssh/atom_key")),
  "workload"  -> new PhysicalInstance("192.168.0.9", "root", new File("/home/user/.ssh/atom_key"))
)

// Create the service configurations.
var configs: Map[String,Map[String,Any]] = Map(
  "rails" -> Map(
    "log_level" -> "debug",
    "ports" -> Map(
      "count" -> 8,
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
  "rails1"  -> new RailsService(instances("webserver_rails1"), configs("rails")),
  "rails2"  -> new RailsService(instances("rails2"), configs("rails")),
  "mysql"   -> new MySQLService(instances("mysql"), configs("mysql")),
  "haproxy" -> new HAProxyService(instances("webserver_rails1"), configs("haproxy")),
  "nginx"   -> new NginxService(instances("webserver_rails1"), configs("nginx")),
  "faban"   -> new FabanService(instances("workload"), configs("faban"))
)

// Configure the service dependencies.
services("rails1").addDependency(services("mysql"))
services("rails1").addDependency(services("haproxy"))
services("rails1").addDependency(services("faban"))
services("rails2").addDependency(services("mysql"))
services("rails2").addDependency(services("haproxy"))
services("rails2").addDependency(services("faban"))
services("haproxy").addDependency(services("rails1"))
services("haproxy").addDependency(services("rails2"))
services("nginx").addDependency(services("haproxy"))
services("faban").addDependency(services("mysql"))
services("faban").addDependency(services("nginx"))

// Start the services.
for (service <- services.values) {
  println(service.recipeName)
  service.start
}
  