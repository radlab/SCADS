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
 * Global state for setting which jars are deployed to ec2.
 */
object MesosCluster {
  //HACK
  var jarFiles: Seq[java.io.File] = _
}

/**
 * Functions to help maintain a mesos cluster on EC2.
 */
class Cluster(val region: EC2Region = EC2East, val useFT: Boolean = false) extends ConfigurationActions {
  /**
   * Location of the deploylib framework on the remote instances
   */
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")

  /**
   * The ami used when launching new instances
   */
  val mesosAmi =
    if (region.endpoint contains "west") "ami-2b6b386e" else "ami-2d60a144"

  /**
   * The default availability zone used when launching new instances.
   */
  val defaultZone =
    if (region.endpoint contains "west") "us-west-1a" else "us-east-1b"

  /**
   * The zookeeper node where the service scheduler registers itself
   */
  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")

  /**
   * A handle to the service scheduler for the cluster
   */
  def serviceScheduler = classOf[RemoteServiceScheduler].newInstance.parse(serviceSchedulerNode.data)

  /**
   * The contents of the service scheduler log for debugging.
   */
  def serviceSchedulerLog = firstMaster.catFile("/root/serviceScheduler.log")

  /**
   * Kills all instances running on EC2 for this mesos cluster cluster (masters, slaves, zookeepers)
   */
  def stopAllInstances() = (masters ++ slaves ++ zooKeepers).pforeach(_.halt)

  /**
   * Start and configure the mesos master, at least numSlaves mesos slaves, and zookeeper in parallel.
   */
  def setup(numSlaves: Int = 1) = Seq(
    Future(setupMesosMaster()),
    Future(setupZooKeeper()),
    Future(if (slaves.size < numSlaves) addSlaves(numSlaves - slaves.size))
  ).map(_())

  /**
   * Update the jars and scripts for deploylib on the specified instances (default: all masters + slaves) using the current classSource
   */
  def updateDeploylib(instances: Seq[EC2Instance] = (slaves ++ masters)): Unit = {
    instances.pforeach(inst => {
      val executorScript = Util.readFile(new File("deploylib/src/main/resources/java_executor"))
        .split("\n")
        .map {
        case s if (s contains "CLASSPATH=") => "CLASSPATH='-cp /usr/local/mesos/lib/java/mesos.jar:" + inst.pushJars(MesosCluster.jarFiles).mkString(":") + "'"
        case s => s
      }.mkString("\n")

      createDirectory(inst, rootDir)
      uploadFile(inst, new File("deploylib/src/main/resources/config"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  /**
   * Configure the mesos master, starting instances if needed.
   */
  def setupMesosMaster(zone: String = defaultZone, numMasters: Int = 1, mesosAmi: Option[String] = None): Unit = {
    if (masters.size < numMasters) {
      mesosAmi match {
        case Some(ami) => startMasters(zone, numMasters - masters.size, ami)
        case None => startMasters(zone, numMasters - masters.size)
      }
    }

    masters.foreach(_.blockUntilRunning)
    masters.pforeach(_.pushJars(MesosCluster.jarFiles))
    updateMasterConf

    zooKeepers.foreach(_.blockUntilRunning)
    zooKeepers.foreach(_.blockTillPortOpen(2181))

    restartMasters
    restartServiceScheduler
  }

  /**
   * Restart the service scheduler daemon running on the mesos master.
   * NOTE: this will kill any other java procs running on the master.
   */
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

  /**
   * Returns a list of EC2Instances for all the slaves in the cluster
   */
  def slaves = {
    region.update()
    region.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "slave")
      .map(t => region.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  /**
   * Returns a list of EC2Instances for all the masters in the cluster
   */
  def masters = {
    region.update()
    region.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "master")
      .map(t => region.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  /**
   * Returns a list of EC2Instances for all the zookeepers in the clsuter
   */
  def zooKeepers = {
    region.update()
    region.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "zoo")
      .map(t => region.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))
  }

  /**
   * Returns he canonical address for the zookeeper quorum for this cluster
   */
  def zooKeeperAddress = "zk://%s/".format(zooKeepers.map(_.publicDnsName + ":2181").mkString(","))

  /**
   * Returns a handle to the root of the the zookeeper quorum for this cluster
   */
  def zooKeeperRoot = ZooKeeperNode(zooKeeperAddress)

  /**
   * Configure all zookepers running to operate in a quorum.
   * Starts at at least numServers if they aren't already running.
   */
  def setupZooKeeper(numServers: Int = 1): Unit = {
    val missingServers = numServers - zooKeepers.size

    if (missingServers > 0) {
      val ret = region.runInstances(
        mesosAmi,
        missingServers,
        missingServers,
        region.keyName,
        "m1.large",
        defaultZone,
        None)

      ret.foreach(_.tags += ("mesos", "zoo"))
      ret.foreach(_.blockUntilRunning)
    }

    val servers = zooKeepers.zipWithIndex

    val serverList = servers.map {
      case (server, id: Int) => "server.%d=%s:3181:3182".format(id + 1, server.privateDnsName)
    }.toList

    val cnf =
      ("dataDir=/mnt/zookeeper" ::
        "clientPort=2181" ::
        "tickTime=1000" ::
        "initLimit=60" ::
        "syncLimit=30" ::
        (if (servers.size > 1) serverList else Nil)).mkString("\n")

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

        server.pushJars(MesosCluster.jarFiles)
        server.executeCommand("killall java")
        server ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/zookeeper.pid --exec /mnt/startZookeeper"
    }

    println(cnf)
  }

  /**
   * Returns the EC2Instance for the first mesos master
   */
  def firstMaster = masters.head

  /**
   * Copy the mesos binaries from the master to all slaves
   */
  def updateMesos =
    slaves.pforeach(s =>
      firstMaster ! "rsync -e 'ssh -o StrictHostKeyChecking=no' -av /usr/local/mesos root@%s:/usr/local/mesos".format(s.publicDnsName))

  /**
   * Returns the mesos cluster url
   */
  def clusterUrl: String =
    if (useFT)
      "zoo://" + zooKeeperRoot.proxy.servers.mkString(",") + zooKeeperRoot.getOrCreate("mesos").path
    else
      "master@" + firstMaster.publicDnsName + ":5050"

  /**
   * Restart the mesos-slave process for all slaves
   */
  def restartSlaves(): Unit = {
    slaves.pforeach(i => {
      i ! "service mesos-slave stop";
      i ! "service mesos-slave start"
    })
  }

  /**
   * Restart the mesos-master process for all masters
   */
  def restartMasters(): Unit = {
    masters.foreach {
      master =>
        master ! "service mesos-master stop"
        master ! "service mesos-master start"
    }
  }

  /**
   * Restart masters, slaves, and the service scheduler.  Also kills any java procs running on slaves.
   */
  def restart(): Unit = {
    restartMasters
    restartSlaves
    restartServiceScheduler
    slaves.pforeach(_.executeCommand("killall java"))
  }

  /**
   * Update the slaves file for the mesos scripts located in /root/mesos-ec2
   */
  @deprecated("don't use the mesos scripts anymore", "v2.1.2")
  def updateSlavesFile: Unit = {
    val location = new File("/root/mesos-ec2/slaves")
    val contents = slaves.map(_.privateDnsName).mkString("\n")
    masters.pforeach {
      master =>
        master.mkdir("/root/mesos-ec2")
        createFile(master, location, contents, "644")
    }
  }

  /**
   * Start and configure the specified number of masters.
   */
  def startMasters(zone: String = defaultZone, count: Int = 1, ami: String = mesosAmi): Seq[EC2Instance] = {
    val ret = region.runInstances(
      ami,
      count,
      count,
      region.keyName,
      "m1.large",
      zone,
      None)

    ret.pforeach(i => {
      i.tags += ("mesos", "master")
      i.blockUntilRunning
      updateSlaveConf(i :: Nil)
    })

    restartMasters
    ret
  }

  /**
   * Add slaves to the cluster
   */
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

    val instances = region.runInstances(
      ami,
      count,
      count,
      region.keyName,
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
        updateSlaveConf(i :: Nil)
        i ! "service mesos-slave restart"
      } catch {
        case e => logger.error(e, "Failed to start slave on instance %s:%s", i.instanceId, i.publicDnsName)
      })
    }

    instances
  }

  protected val baseConf = (
    "webui_port=8080" ::
      "work_dir=/mnt" ::
      "log_dir=/mnt" ::
      "switch_user=0" ::
      "shares_interval=30" :: Nil)

  protected def confWithUrl = ("master=" + clusterUrl) :: baseConf

  /**
   * Returns the contents of the mesos slave configuration file.
   */
  def slaveConf(instance: EC2Instance) = (("mem=" + (instance.free.total - 1024)) ::
    confWithUrl).mkString("\n")

  /**
   * the location of the configuration file for both masters and slaves on the remote machine
   */
  val confFile = new File("/usr/local/mesos/conf/mesos.conf")


  /**
   * Updates the slave configuration file on the specified instances (default all slaves)
   */
  def updateSlaveConf(instances: Seq[EC2Instance] = slaves): Unit = {
    instances.pforeach(i => i.createFile(confFile, slaveConf(i)))
  }

  /**
   * Updates the master configuration file on all masters.
   */
  def updateMasterConf: Unit = {
    val masterConf =
      if (useFT)
        confWithUrl.mkString("\n")
      else
        baseConf.mkString("\n")

    masters.pforeach(_.createFile(confFile, masterConf))
  }

  /**
   * Returns S3 Cached jars for all jar files specified in MesosCluster.jarFiles.  This will upload any jar files that don't already exist
   */
  def classSource: Seq[S3CachedJar] =
    if (System.getProperty("deploylib.classSource") == null)
      pushJars.map(_.getName)
        .map(S3Cache.hashToUrl)
        .map(new S3CachedJar(_))
    else
      System.getProperty("deploylib.classSource").split("\\|").map(S3CachedJar(_))

  //TODO fix me.
  def pushJars: Seq[String] = {
    val jars = MesosCluster.jarFiles
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

  /**
   * Tail stdout of the most recently started framework on all slaves and print the output to stdout of the local machine.
   * Used for debugging.
   */
  def tailSlaveLogs: Unit = {
    val workDir = new File("/mnt/work")
    slaves.pmap(s => {
      val currentSlaveDir = new File(workDir, s.ls(workDir).sortBy(_.modDate).last.name)
      val currentFrameworkDir = new File(currentSlaveDir, s.ls(currentSlaveDir).head.name)
      (s, new File(currentFrameworkDir, "0/stdout"))
    }).foreach {
      case (s, l) => s.watch(l)
    }
  }
}
