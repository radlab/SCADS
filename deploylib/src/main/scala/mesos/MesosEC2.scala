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
class Cluster(useFT: Boolean = false) extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")

  val mesosAmi =
    if (EC2Instance.endpoint contains "west") "ami-2b6b386e" else "ami-5af60d33"

  val defaultZone =
    if (EC2Instance.endpoint contains "west") "us-west-1a" else "us-east-1b"

  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")

  def serviceScheduler = classOf[RemoteServiceScheduler].newInstance.parse(serviceSchedulerNode.data)
  def serviceSchedulerLog = firstMaster.catFile("/root/serviceScheduler.log")

  def stopAllInstances = (masters ++ slaves ++ zooKeepers).pforeach(_.halt)

  def setup(numSlaves: Int = 1) = (Future {setupMesosMaster()} ::
                                   Future {setupZooKeeper()} ::
                                   Future {if(slaves.size < numSlaves) addSlaves(numSlaves - slaves.size)} :: Nil).map(_())
	
  def updateDeploylib(instances: Seq[EC2Instance] = slaves): Unit = {
    instances.pforeach(inst => {
      val executorScript = Util.readFile(new File("deploylib/src/main/resources/java_executor"))
        .split("\n")
        .map {
        case s if (s contains "CLASSPATH=") => "CLASSPATH='-cp /usr/local/mesos/lib/java/mesos.jar:" + inst.pushJars.mkString(":") + "'"
        case s => s
      }.mkString("\n")

      createDirectory(inst, rootDir)
      uploadFile(inst, new File("deploylib/src/main/resources/config"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  def setupMesosMaster(zone: String = defaultZone, numMasters: Int = 1, mesosAmi: Option[String] = None): Unit = {
    if (masters.size < numMasters) {
      mesosAmi match {
        case Some(ami) => startMasters(zone, numMasters - masters.size, ami)
        case None => startMasters(zone, numMasters - masters.size)
      }
    }

    masters.foreach(_.blockUntilRunning)
    masters.pforeach(_.pushJars)
    updateMasterConf

    zooKeepers.foreach(_.blockUntilRunning)
    zooKeepers.foreach(_.blockTillPortOpen(2181))

    restartMasters
    restartServiceScheduler
  }

  def restartServiceScheduler: Unit = {
    zooKeepers.foreach(_.blockUntilRunning)
    masters.pforeach(_.executeCommand("killall java"))
    val serviceSchedulerScript = (
      "#!/bin/bash\n" +
        "/root/jrun -Dscads.comm.externalip=true deploylib.mesos.ServiceSchedulerDaemon " +
        "--mesosMaster " + clusterUrl + " " +
        "--zooKeeperAddress " + serviceSchedulerNode.canonicalAddress +
        " >> /root/serviceScheduler.log 2>&1")

    // Start service scheduler on only the first master
    firstMaster.createFile(new java.io.File("/root/serviceScheduler"), serviceSchedulerScript)
    firstMaster ! "chmod 755 /root/serviceScheduler"
    firstMaster ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/serviceScheduler.pid --exec /root/serviceScheduler"
  }

  def slaves = {
    EC2Instance.update()
    EC2Instance.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "slave")
      .map(t => EC2Instance.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  def masters = {
    EC2Instance.update()
    EC2Instance.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "master")
      .map(t => EC2Instance.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  def zooKeepers = {
    EC2Instance.update()
    EC2Instance.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "zoo")
      .map(t => EC2Instance.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  def zooKeeperAddress = "zk://%s/".format(zooKeepers.map(_.publicDnsName + ":2181").mkString(","))
  def zooKeeperRoot = ZooKeeperNode(zooKeeperAddress)

  def setupZooKeeper(): Unit = {
    val missingServers = 3 - zooKeepers.size

    if(missingServers > 0) {
    val ret = EC2Instance.runInstances(
      mesosAmi,
      missingServers,
      missingServers,
      EC2Instance.keyName,
      "m1.large",
      defaultZone,
      None)

      ret.foreach(_.tags += ("mesos", "zoo"))
      ret.foreach(_.blockUntilRunning)
    }

    val servers = zooKeepers.zipWithIndex

    val cnf =
      (servers.map {case (server, id: Int) => "server.%d=%s:3181:3182".format(id + 1, server.privateDnsName)} ++
      ("dataDir=/mnt/zookeeper" ::
      "clientPort=2181" ::
      "tickTime=1000" ::
      "initLimit=60"::
      "syncLimit=30" :: Nil )).mkString("\n")

    val startScript =
      ("#!/bin/bash" ::
      "/root/jrun org.apache.zookeeper.server.quorum.QuorumPeerMain /mnt/zookeeper.cnf" :: Nil).mkString("\n")

    servers.pforeach {
      case (server, id: Int) =>
        server ! "mkdir -p /mnt/zookeeper"
        server.createFile(new File("/mnt/zookeeper.cnf"), cnf)
        server.createFile(new File("/mnt/zookeeper/myid"), (id + 1).toString)
        server.createFile(new File("/mnt/startZookeeper"), startScript)
        server ! "chmod 755 /mnt/startZookeeper"

        server.pushJars
        server.executeCommand("killall java")
        server ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/zookeeper.pid --exec /mnt/startZookeeper"
    }

    println(cnf)
  }

  def firstMaster = masters.head

  def updateMesos =
    slaves.pforeach(s =>
      firstMaster ! "rsync -e 'ssh -o StrictHostKeyChecking=no' -av /usr/local/mesos root@%s:/usr/local/mesos".format(s.publicDnsName))

  def clusterUrl: String =
    if(useFT)
      "zoo://" + zooKeeperRoot.proxy.servers.mkString(",") + zooKeeperRoot.getOrCreate("mesos").path
    else
      "master@" + firstMaster.publicDnsName + ":5050"

  def restartSlaves(): Unit = {
    slaves.pforeach(i => {i ! "service mesos-slave stop"; i ! "service mesos-slave start"})
  }

  def restartMasters(): Unit = {
    masters.foreach {
      master =>
        master ! "service mesos-master stop"
        master ! "service mesos-master start"
    }
  }

  def restart(): Unit = {
    restartMasters
    restartSlaves
    restartServiceScheduler
    slaves.pforeach(_ ! "killall java")
  }

  def updateSlavesFile: Unit = {
    val location = new File("/root/mesos-ec2/slaves")
    val contents = slaves.map(_.privateDnsName).mkString("\n")
    masters.pforeach {
      master =>
        master.mkdir("/root/mesos-ec2")
        createFile(master, location, contents, "644")
    }
  }

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
        try {
	      masters.foreach(_.blockUntilRunning)
          Some("url=" + clusterUrl) 
		} catch {
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
	instances.foreach(_.tags += ("mesos", "slave"))

    if (updateDeploylibOnStart) {
      //Hack to avoid race condition where there are no masters yet because they haven't been tagged.
      instances.head.blockUntilRunning
      masters.foreach(_.blockUntilRunning)
      firstMaster.blockTillPortOpen(5050)

      instances.pforeach(i => try {
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

  val baseConf = ("work_dir=/mnt" ::
      "log_dir=/mnt" ::
      "switch_user=0" ::
      "shares_interval=30" :: Nil)
  def slaveConf = (baseConf :+ ("url=" + clusterUrl)).mkString("\n")
  val conffile = new File("/usr/local/mesos/conf/mesos.conf")

  def updateConf(instances: Seq[EC2Instance] = slaves): Unit = {
    instances.pforeach(_.createFile(conffile, slaveConf))
  }

  def updateMasterConf: Unit = {
    val masterConf =
      if(useFT)
        slaveConf
      else
        baseConf.mkString("\n")

    masters.pforeach(_.createFile(conffile, masterConf))
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
