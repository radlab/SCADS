package deploylib
package mesos

import ec2._
import config._

import edu.berkeley.cs.scads.comm._

import java.io.File
import java.net.InetAddress
import collection.JavaConversions._
import com.amazonaws.services.ec2.model._

object ServiceSchedulerDaemon extends optional.Application {
  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"

  def main(mesosMaster: String, zooKeeperAddress: String): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new ServiceScheduler(
      mesosMaster,
      javaExecutorPath
    )
    val serviceSchedulerNode = ZooKeeperNode(zooKeeperAddress)
    serviceSchedulerNode.data = scheduler.remoteHandle.toBytes
  }
}

/**
 * Functions to help maintain a mesos cluster on EC2.
 */
class Cluster(val zooKeeperRoot: ZooKeeperProxy#ZooKeeperNode) extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")
  val mesosAmi = "ami-44ce3d2d"
  val zone = "us-east-1a"

  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")
  def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)

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

  def setupMesosMaster(zone:String = zone, numMasters: Int = 1, mesosAmi: Option[String] = None): Unit = {
    if(masters.size < numMasters) {
      mesosAmi match {
        case Some(ami) => startMasters(zone, numMasters - masters.size, ami)
        case None => startMasters(zone, numMasters - masters.size)
      }
    }

    masters.pforeach(_.pushJars)
    updateConf(masters)
    restartMasters
    restartServiceScheduler
  }

  def restartServiceScheduler: Unit = {
    masters.pforeach(_.executeCommand("killall java"))
    val serviceSchedulerScript = (
      "#!/bin/bash\n" +
      "/root/jrun deploylib.mesos.ServiceSchedulerDaemon " +
      "--mesosMaster " + clusterUrl + " " +
      "--zooKeeperAddress " + serviceSchedulerNode.canonicalAddress +
      " >> /root/serviceScheduler.log 2>&1")

    // Start service scheduler on only the first master
    firstMaster.createFile(new java.io.File("/root/serviceScheduler"), serviceSchedulerScript)
    firstMaster ! "chmod 755 /root/serviceScheduler"
    firstMaster ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/serviceScheduler.pid --exec /root/serviceScheduler"

    //HACK
    Thread.sleep(5000)
    serviceSchedulerNode.data = RemoteActor(firstMaster.publicDnsName, 9000, ActorNumber(0)).toBytes
  }

  def slaves = {
    EC2Instance.update()
    EC2Instance.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "slave")
      .map(t => EC2Instance.getInstance(t.getResourceId))
      .filter(_.instanceState equals "running")
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
    slaves.pforeach(s =>
      firstMaster ! "rsync -e 'ssh -o StrictHostKeyChecking=no' -av /usr/local/mesos root@%s:/usr/local/mesos".format(s.publicDnsName))

  def clusterUrl: String =
    "zoo://" + zooKeeperRoot.proxy.servers.map(_ + ":2181").mkString(",") + zooKeeperRoot.getOrCreate("mesos").path

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
      instances.pforeach(i => try {
	i.tags += ("mesos", "slave")
	i.blockUntilRunning
	updateDeploylib(i :: Nil)
	updateConf(i :: Nil)
	i ! "service mesos-slave restart"
      } catch {
	case e => logger.error(e, "Failed to start slave on instance %s:%s", i.instanceId, i.publicDnsName)
      })
    }

    instances
  }

  def updateConf(instances: Seq[EC2Instance] = (slaves ++ masters)): Unit = {
    val baseConf = ("work_dir=/mnt" ::
      "log_dir=/mnt" ::
      "switch_user=0" ::
      "shares_interval=30" :: Nil)
    val conf = (baseConf :+ ("url=" + clusterUrl)).mkString("\n")
    val conffile = new File("/usr/local/mesos/conf/mesos.conf")

    instances.pforeach(_.createFile(conffile, conf))
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
