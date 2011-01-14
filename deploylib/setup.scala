import net.lag.logging.Logger
import deploylib._
import deploylib.ec2._
import deploylib.mesos._
import deploylib.rcluster._
import net.lag.configgy._

implicit def toFile(str: String) = new java.io.File(str)

def debug = Logger("deploylib").setLevel(java.util.logging.Level.FINEST)

def stopAllInstances: Unit = {
  EC2Instance.activeInstances.foreach(_.halt)
}
