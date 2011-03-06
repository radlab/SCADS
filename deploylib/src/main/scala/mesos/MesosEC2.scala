package deploylib
package mesos

import ec2._
import config._

import edu.berkeley.cs.scads.comm._

import java.io.File
import java.net.InetAddress
import collection.JavaConversions._
import com.amazonaws.services.ec2.model._

/**
 * Functions to help maintain a mesos cluster on EC2.
 */
object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")
  val mesosAmi = "ami-44ce3d2d"

  def updateDeploylib(instances: Seq[EC2Instance] = slaves): Unit = {
    instances.pforeach(inst => {
      val executorScript = Util.readFile(new File("deploylib/src/main/resources/java_executor"))
      .split("\n")
      .map {
	case s if(s contains "CLASSPATH=") => "CLASSPATH='-cp /usr/local/mesos/lib/java/mesos.jar:" + inst.pushJars.mkString(":") + "'"
	case s => s
      }.mkString("\n")

      createDirectory(inst, rootDir)
      uploadFile(inst, new File("deploylib/src/main/resources/config"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  def slaves = {
    val masterIds = masters.map(_.instanceId)
    EC2Instance.activeInstances.filterNot(i => masterIds.contains(i.instanceId))
  }

  def masters = {
    EC2Instance.update()
    EC2Instance.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "master")
      .map(t => EC2Instance.getInstance(t.getResourceId))
      .filter(_.instanceState equals "running")
  }

  def firstMaster = masters.head

  def updateMesos =
    MesosEC2.slaves.pforeach(s =>
      MesosEC2.firstMaster ! "rsync -e 'ssh -o StrictHostKeyChecking=no' -av /usr/local/mesos root@%s:/usr/local/mesos".format(s.publicDnsName))

  def clusterUrl: String = {
    val masters = MesosEC2.masters
    if (masters.size == 1)
      "1@" + firstMaster.privateDnsName + ":5050"
    else
      "zoo://ec2-50-16-2-36.compute-1.amazonaws.com:2181/mesos,ec2-174-129-105-138.compute-1.amazonaws.com:2181/mesos"
  }

  def restartSlaves: Unit = {
    slaves.pforeach(_ ! "service mesos-slave stop")
    slaves.pforeach(_ ! "service mesos-slave start")
  }

  def restartMasters: Unit = {
    masters.foreach { master =>
      master ! "service mesos-master stop"
      master ! "service mesos-master start"
    }
  }

  def restart: Unit = {
    restartMasters
    restartSlaves
  }

  def updateSlavesFile: Unit = {
    val location = new File("/root/mesos-ec2/slaves")
    val contents = slaves.map(_.privateDnsName).mkString("\n")
    masters.pforeach { master =>
      master.mkdir("/root/mesos-ec2")
      createFile(master, location, contents, "644")
    }
  }

  val defaultZone = "us-east-1a"
  def startMasters(zone: String = defaultZone, count: Int = 1, ami: String = mesosAmi): Seq[EC2Instance] = {
    val ret = EC2Instance.runInstances(
      ami,
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      zone,
      None)

    ret.pforeach(i => {
      i.tags += ("mesos", "master")
      i.blockUntilRunning
      updateConf(i :: Nil)
    })

    restartMasters
    ret
  }

  def addSlaves(count: Int, zone: String = defaultZone, ami: String = mesosAmi, updateDeploylibOnStart: Boolean = true): Seq[EC2Instance] = {
    val userData =
      if (updateDeploylibOnStart)
        None
      else
        try Some("url=" + clusterUrl) catch {
          case noMaster: java.util.NoSuchElementException =>
            logger.warning("No master found. Starting without userdata")
            None
        }

    val instances = EC2Instance.runInstances(
      ami,
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      zone,
      userData)

    if(updateDeploylibOnStart) {
      instances.pforeach(i => {
	i.blockUntilRunning
	updateDeploylib(i :: Nil)
	updateConf(i :: Nil)
	i ! "service mesos-slave start"
      })
    }

    instances
  }

  def updateConf(instances: Seq[EC2Instance] = (slaves ++ masters)): Unit = {
    val baseConf = ("work_dir=/mnt" ::
      "log_dir=/mnt" ::
      "switch_user=0" ::
      "shares_interval=30" :: Nil)

    val mastersConf = baseConf.mkString("\n")
    val slavesConf = (baseConf :+ ("url=" + clusterUrl)).mkString("\n")
    val conffile = new File("/usr/local/mesos/conf/mesos.conf")

    slaves.pforeach(_.createFile(conffile, slavesConf))
    masters.pforeach(_.createFile(conffile, mastersConf))
  }

  //TODO: Doesn't handle non s3 cached jars
  def classSource: Seq[S3CachedJar] =
    if (System.getProperty("deploylib.classSource") == null)
      pushJars.map(_.getName)
        .map(S3Cache.hashToUrl)
        .map(new S3CachedJar(_))
    else
      System.getProperty("deploylib.classSource").split("\\|").map(S3CachedJar(_))

  def pushJars: Seq[String] = {
    val jarFile = new File("allJars")
    val jars = Util.readFile(jarFile).split("\n").map(new File(_))
    val (deploylib, otherJars) = jars.partition(_.getName contains "deploylib")
    val sortedJars = deploylib ++ otherJars

    logger.debug("Starting Jar upload")
    sortedJars.map(S3Cache.getCacheUrl)
  }

  /**
   * Create a public key on the master if it doesn't exist
   * Then add that to the authorized key file all of slaves
   * TODO: Dedup keys
   */
  def authorizeMaster: Unit = {
    val getKeyCommand = "cat /root/.ssh/id_rsa.pub"
    val key = try (firstMaster !? getKeyCommand) catch {
      case u: UnknownResponse => {
        firstMaster ! "ssh-keygen -t rsa -f /root/.ssh/id_rsa -N \"\""
        firstMaster !? getKeyCommand
      }
    }

    slaves.pforeach(_.appendFile(new File("/root/.ssh/authorized_keys"), key))
  }
}
