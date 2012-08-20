package deploylib
package mesos

import ec2._
import config._

import edu.berkeley.cs.scads.comm._

import java.io.File
import collection.JavaConversions._
import net.lag.logging.Logger
import java.lang.RuntimeException

import scala.actors.Future
import scala.actors.Futures._

object ServiceSchedulerDaemon extends optional.Application {
  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"

  def main(mesosMaster: String, zooKeeperAddress: String): Unit = {
    System.loadLibrary("mesos")
    val scheduler = new ServiceScheduler(
      mesosMaster,
      javaExecutorPath
    )
    val serviceSchedulerNode = ZooKeeperNode(zooKeeperAddress)
    val remoteService = scheduler.remoteHandle
    serviceSchedulerNode.data = new RemoteServiceScheduler(remoteService.remoteNode, remoteService.id).toBytes
  }
}

/**
 * Global state for setting and methods for running mesos on EC2.
 */
object MesosCluster {
  val logger = Logger()

  //HACK
  var jarFiles: Seq[java.io.File] = _

  /**
   * Start a new plain ubuntu ami, install java/mesos on it, bundle it as a new AMI.
   */
  def buildNewAmi(region: EC2Region): String = {
    region.update()
    val oldInst = region.client.describeTags().getTags()
      .filter(_.getResourceType equals "instance")
      .filter(_.getKey equals "mesos")
      .filter(_.getValue equals "ami")
      .map(t => region.getInstance(t.getResourceId))
      .filter(i => (i.instanceState equals "running") || (i.instanceState equals "pending"))

    val inst =
      if (oldInst.size == 0)
        region.runInstances(1).head
      else
        oldInst.head

    inst.tags += ("mesos", "ami")
    inst.blockUntilRunning()

    logger.info("updating apt...")
    inst ! "add-apt-repository \"deb http://archive.canonical.com/ubuntu lucid partner\""
    inst ! "echo sun-java6-jdk shared/accepted-sun-dlj-v1-1 select true | /usr/bin/debconf-set-selections"
    inst ! "apt-get update"
    logger.info("installing java...")
    inst ! "yes | apt-get install -y sun-java6-jdk"
    logger.info("installing build env...")
    inst ! "apt-get install -y build-essential"
    logger.info("installing git...")
    inst ! "apt-get install -y git-core"
    logger.info("installing python...")
    inst ! "apt-get install -y python2.6-dev"
    logger.info("installing swig...")
    inst ! "apt-get install -y swig"
    logger.info("installing autoconf...")
    inst ! "apt-get install -y autoconf"
    logger.info("cloning mesos")
    inst ! "git clone https://github.com/mesos/mesos.git"
    logger.info("building and installing mesos...")
    inst ! "cd mesos; ./configure --with-python-headers=/usr/include/python2.6 --with-java-home=/usr/lib/jvm/java-6-sun --with-webui --with-included-zookeeper; make; make install"

    val bucketName = (region.getClass.getName.dropRight(1) + System.currentTimeMillis).toLowerCase.replaceAll("\\.", "")
    logger.info("bundling ami as %s...", bucketName)
    val ami = inst.bundleNewAMI(bucketName)
    inst.halt
    ami
  }
}

/**
 * Functions to help maintain a mesos cluster on EC2.
 */
class Cluster(val region: EC2Region = USEast1, val useFT: Boolean = false) {
  val logger = Logger()

  /**
   * The location of mesos on the remote machine
   */
  val mesosDir = new File("/usr/local/mesos")

  /**
   * Location of the deploylib framework on the remote instances
   */
  val frameworkDir = new File(mesosDir, "frameworks/deploylib")

  val binDir = new File(mesosDir, "bin")

  /**
   * The ami used when launching new instances
   */
  val mesosAmi = region match {
    case EC2East => "ami-d373b1ba"
    case EC2West => "ami-1f08545a"
    case USEast1 => "ami-d373b1ba"
    case USWest1 => "ami-1f08545a"
    case EUWest1 => "ami-43b38137"
    case APNortheast1 => "ami-04912505"
    case APSoutheast1 => "ami-92057fc0"
  }

  /**
   * The default availability zone used when launching new instances.
   */
  def availabilityZone: Option[String] = synchronized {
    val activeZones = (masters ++ slaves ++ zooKeepers).map(_.availabilityZone).toSet
    if (activeZones.size == 1)
      Some(activeZones.head)
    else if (activeZones.size == 0) {
      logger.info("Starting a master to determine a good zone to run in.")
      startMasterInstances(1)
      while(masters.size == 0) {
        Thread.sleep(1000)
        region.forceUpdate()
      }
      Some(masters.head.availabilityZone)
    } else
      throw new RuntimeException("Cluster is split across zones: " + activeZones)
  }

  /**
   * The zookeeper node where the service scheduler registers itself
   */
  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")

  /**
   * A handle to the service scheduler for the cluster
   */
  def serviceScheduler = classOf[RemoteServiceScheduler].newInstance.parse(serviceSchedulerNode.data)

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

      inst.mkdir(frameworkDir)
      inst.upload(new File("deploylib/src/main/resources/config"), frameworkDir)
      inst.createFile(new File(frameworkDir, "java_executor"), executorScript, "755")
    })
  }

  /**
   * Configure the mesos master, starting instances if needed.
   */
  def setupMesosMaster(numMasters: Int = 1): Unit = {
    //figure out the zone to prevent double master start
    val zone = availabilityZone

    if (masters.size < numMasters)
      startMasters(numMasters - masters.size)

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
    val serviceSchedulerCmd =
      "/$HOME/jrun -Dscads.comm.externalip=true deploylib.mesos.ServiceSchedulerDaemon " +
        "--mesosMaster " + clusterUrl + " " +
        "--zooKeeperAddress " + serviceSchedulerNode.canonicalAddress

    firstMaster.getService("service-scheduler", serviceSchedulerCmd).restart
  }

  protected def slaveService(inst: EC2Instance): ServiceManager#RemoteService = inst.getService("mesos-slave", new File(binDir, "mesos-slave").getCanonicalPath)

  def slaveServices = slaves.map(slaveService)

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

  def masterServices = masters.map(_.getService("mesos-master", new File(binDir, "mesos-master").getCanonicalPath))

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
        availabilityZone,
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

    val startCommand = "/$HOME/jrun org.apache.zookeeper.server.quorum.QuorumPeerMain /mnt/zookeeper.cnf"

    servers.pforeach {
      case (server, id: Int) =>
        server ! "mkdir -p /mnt/zookeeper"
        server.createFile(new File("/mnt/zookeeper.cnf"), cnf)
        server.createFile(new File("/mnt/zookeeper/myid"), (id + 1).toString)

        val zooService = server.getService("zookeeper", startCommand)

        server.pushJars(MesosCluster.jarFiles)
        server.executeCommand("killall java")
        zooService.start
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
  def restartSlaves(): Unit = slaveServices.pforeach(_.restart)

  /**
   * Restart the mesos-master process for all masters
   */
  def restartMasters(): Unit = masterServices.pforeach(_.restart)

  def killMasters(): Unit = {
    println("masters: killall -9 mesos-master")
    masters.pforeach(_ executeCommand "killall -9 mesos-master")
    println("masters: killall -9 java")
    masters.pforeach(_ executeCommand "killall -9 java")
  }

  def killSlaves(): Unit = {
    println("slaves: killall -9 mesos-slave")
    slaves.pforeach(_ executeCommand "killall -9 mesos-slave")
    println("slaves: killall -9 java")
    slaves.pforeach(_.executeCommand("killall -9 java"))
  }

  /**
   * Restart masters, slaves, and the service scheduler.  Also kills any java procs running on slaves.
   */
  def restart(): Unit = {
//    masters.pforeach(_ executeCommand "killall -9 mesos-master")
//    masters.pforeach(_ executeCommand "killall -9 java")
//    slaves.pforeach(_ executeCommand "killall -9 mesos-slave")
//    slaves.pforeach(_.executeCommand("killall -9 java"))
    val f1 = future { killMasters; println("restartMasters"); restartMasters }
    val f2 = future { killSlaves; println("restartSlaves"); restartSlaves }
    f1()

//    restartMasters
//    restartSlaves
    println("restartServiceScheduler")
    restartServiceScheduler
    println("restartServiceSchedulerDone")

    f2()
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
        master.createFile(location, contents, "644")
    }
  }

  protected def startMasterInstances(count: Int, zone: Option[String] = None): Seq[EC2Instance] = {
    val instances = region.runInstances(
      mesosAmi,
      count,
      count,
      region.keyName,
      "m1.large",
      zone,
      None)
    instances.foreach(_.tags += ("mesos", "master"))
    instances
  }

  /**
   * Start and configure the specified number of masters.
   */
  def startMasters(count: Int = 1, ami: String = mesosAmi): Seq[EC2Instance] = {
    val instances = startMasterInstances(count, availabilityZone)

    instances.pforeach(i => {
      i.blockUntilRunning
      updateSlaveConf(i :: Nil)
    })

    restartMasters
    instances
  }

  /**
   * Add slaves to the cluster
   */
  def addSlaves(count: Int, updateDeploylibOnStart: Boolean = true): Seq[EC2Instance] = {
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
      mesosAmi,
      count,
      count,
      region.keyName,
      "m1.large",
      availabilityZone,
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
        slaveService(i).start
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
    val getKeyCommand = "cat /$HOME/.ssh/id_rsa.pub"
    val key = try (firstMaster !? getKeyCommand) catch {
      case u: UnknownResponse => {
        firstMaster ! "ssh-keygen -t rsa -f /$HOME/.ssh/id_rsa -N \"\""
        firstMaster !? getKeyCommand
      }
    }

    slaves.pforeach(_.appendFile(new File("/$HOME/.ssh/authorized_keys"), key))
  }

  @deprecated("use watchSlaveLogs", "v2.1.2")
  def tailSlaveLogs = watchSlaveLogs

  /**
   * Watch stdout of the most recently started framework on all slaves and print the output to stdout of the local machine.
   * Used for debugging.
   */
  def watchSlaveLogs: Unit = {
    val workDir = new File("/mnt/work")
    slaves.pforeach(s => {
      try {
        val currentSlaveDir = new File(workDir, s.ls(workDir).sortBy(_.modDate).last.name)
        val currentFrameworkDir = new File(currentSlaveDir, s.ls(currentSlaveDir).head.name)
        s.watch(new File(currentFrameworkDir, "0/stdout"))
      }
      catch {
        case e => logger.warning("No frameworks on %s", s.publicDnsName)
      }
    })
  }

  /**
   * Watch stderr of the most recently started framework on all slaves and print the output to stdout of the local machine.
   * Used for debugging.
   */
  def watchExecLogs: Unit = {
    val workDir = new File("/mnt/work")
    slaves.pforeach(s => {
      try {
        val currentSlaveDir = new File(workDir, s.ls(workDir).sortBy(_.modDate).last.name)
        val currentFrameworkDir = new File(currentSlaveDir, s.ls(currentSlaveDir).head.name)
        s.watch(new File(currentFrameworkDir, "0/stderr"))
      }
      catch {
        case e => logger.warning("No frameworks on %s", s.publicDnsName)
      }
    })
  }
}
