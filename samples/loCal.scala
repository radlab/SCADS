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
  "webserver" -> new PhysicalInstance("192.168.0.27", "root", new File("/home/user/.ssh/atom_key")),
  "mysql" -> new PhysicalInstance("192.168.0.28", "root", new File("/home/user/.ssh/atom_key")),
  "workload" -> new PhysicalInstance("192.168.0.29", "root", new File("/home/user/.ssh/atom_key")),
//  "workload"  -> new PhysicalInstance("192.168.0.9", "root", new File("/home/user/.ssh/atom_key"))
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
  "rails"  -> new RailsService(instances("webserver"), configs("rails")),
  "mysql"   -> new MySQLService(instances("mysql"), configs("mysql")),
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
services("mysql").start
services("rails").start
services("haproxy").start
services("nginx").start
services("faban").start
