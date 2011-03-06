package edu.berkeley.cs
package radlab

import java.io.File

package object demo {
  import DemoConfig._
  import scads.comm._
  import scads.storage._
  import scads.perf._
  import scads.piql._
  import scads.storage._
  import twitterspam._
  import deploylib.mesos._
  import scads.piql.modeling._

  val logger = net.lag.logging.Logger()

  lazy val twitterSpamData = new ScadsCluster(twitterSpamRoot).getNamespace[TwitterSpamRecord]("twitterSpamRecords")

  protected val executor = new ParallelExecutor
  lazy val scadrClient = new scadr.ScadrClient(new ScadsCluster(scadrRoot), executor)
  lazy val graditClient = new gradit.GraditClient(new ScadsCluster(graditRoot), executor)
  lazy val comradesClient = new comrades.ComradesClient(new ScadsCluster(comradesRoot), executor)

  def runDemo: Unit = {
    resetScads
    startScadrCluster()
    startGraditCluster()
  }

  /**
   * Needs to be run from Michael's AWS account
   */
  def updateLoadBalancers: Unit = {
    LoadBalancer.update("scadr", scadrWebServerList)
    LoadBalancer.update("gradit", graditWebServerList)
    LoadBalancer.update("comrades", comradesWebServerList)
    LoadBalancer.update("mesos", MesosEC2.firstMaster.instanceId :: Nil)
  }

  /**
   * Start a mesos master and make it the primary for the demo.
   * Only needs to be run by one person.
   */
  def setupMesosMaster(zone:String = zone, numMasters: Int = 1, mesosAmi: Option[String] = None): Unit = {
    if(MesosEC2.masters.size < numMasters) {
      mesosAmi match {
        case Some(ami) => MesosEC2.startMasters(zone, numMasters - MesosEC2.masters.size, ami)
        case None =>  MesosEC2.startMasters(zone, numMasters - MesosEC2.masters.size)
      }
    }

    MesosEC2.masters.pforeach(_.pushJars)
    restartMasterProcs
    mesosMasterNode.data = MesosEC2.clusterUrl.getBytes
  }

  val pidIpRegEx = """\d+@([^:]+):\d+""".r
  def failOverMesosMaster: Unit = {
    val currentPid = new String(zooKeeperRoot
				.proxy
				.root.children
				.find(_.name contains "mesos").get("mesos")
				.children.sortWith(_.name < _.name).head.data)
    val currentIp =  currentPid match {
      case pidIpRegEx(internalIp) => internalIp
      case _ => throw new RuntimeException("Unable to locate ip address of current mesos master")
    }

    val currentMaster = MesosEC2.masters
			.find(_ !? "hostname -i" contains currentIp)
			.getOrElse(throw new RuntimeException("Master not found in list of active masters"))

    logger.info("Restarting master on: %s", currentMaster.publicDnsName)
    currentMaster ! "service mesos-master restart"
  }

  def preloadWars: Unit = {
    MesosEC2.slaves.pforeach(_.cacheFiles(scadrWarFile :: graditWarFile :: Nil))
  }

  /**
   * Restart the service meta-scheduler/mesos utilization reporter on the mesos master.
   * Note this should only be run by the cluster owner, and it will kill all running java procs on the master.
   */
  def restartMasterProcs: Unit = {
    MesosEC2.masters.pforeach(_.executeCommand("killall java"))

    // Start service scheduler on only the first master
    MesosEC2.firstMaster.createFile(new java.io.File("/root/serviceScheduler"), "#!/bin/bash\n/root/jrun edu.berkeley.cs.radlab.demo.ServiceSchedulerDaemon --mesosMaster " + MesosEC2.clusterUrl + " >> /root/serviceScheduler.log 2>&1")
    MesosEC2.firstMaster ! "chmod 755 /root/serviceScheduler"
    MesosEC2.firstMaster ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/serviceScheduler.pid --exec /root/serviceScheduler"

    // Start utilization reporter on all masters (so we can get stats after a failover)
    MesosEC2.masters.pforeach { master =>
      master.createFile(new java.io.File("/root/utilizationReporter"), "#!/bin/bash\ntail -F /tmp/mesos-shares | /root/jrun edu.berkeley.cs.radlab.demo.MesosClusterShareReporter '" + dashboardDb + "' >> /root/utilizationReporter.log 2>&1")
      master ! "chmod 755 /root/utilizationReporter"
      master ! "start-stop-daemon --make-pidfile --start --background --pidfile /var/run/utilizationReporter.pid --exec /root/utilizationReporter"
    }

    //HACK
    Thread.sleep(5000)
    serviceSchedulerNode.data = RemoteActor(MesosEC2.firstMaster.publicDnsName, 9000, ActorNumber(0)).toBytes
  }

  def safeUrl(cs: S3CachedJar, delim: String = "|"): String = {
    if (cs.url.contains(delim))
      throw new RuntimeException("delim cannot be used b/c of url: %s".format(cs.url))
    cs.url
  }

  def startScadr: Unit = {
    val task = WebAppSchedulerTask(
      "SCADr",
      mesosMaster,
      javaExecutorPath,
      scadrWar,
      scadrWebServerList.canonicalAddress,
      Map("scads.clusterAddress" -> scadrRoot.canonicalAddress,
	      "demo.appname" -> "scadr")).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startGradit: Unit = {
    val task = WebAppSchedulerTask(
      "gRADit",
      mesosMaster,
      javaExecutorPath,
      graditWar,
      graditWebServerList.canonicalAddress,
    Map("scads.clusterAddress" -> graditRoot.canonicalAddress,
	"demo.appname" -> "gradit")).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startComrades: Unit = {
    val task = WebAppSchedulerTask(
      "comRADes",
      mesosMaster,
      javaExecutorPath,
      comradesWar,
      comradesWebServerList.canonicalAddress,
    Map("scads.clusterAddress" -> comradesRoot.canonicalAddress,
        "demo.appname" -> "comrades")).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startScadrCluster(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(scadrRoot.canonicalAddress)
    val task = InitScadrClusterTask(clusterAddress).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startGraditCluster(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(graditRoot.canonicalAddress)
    val task = InitGraditClusterTask(clusterAddress).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startComradesCluster(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(comradesRoot.canonicalAddress)
    val task = InitComradesClusterTask(clusterAddress).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startScadrDirector(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(scadrRoot.canonicalAddress)
    val task = ScadrDirectorTask(
      clusterAddress,
      mesosMaster
    ).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startGraditDirector(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(graditRoot.canonicalAddress)
    val task = GraditDirectorTask(
      clusterAddress,
      mesosMaster
    ).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startTraceCollector: Unit = {
    println("starting trace collection...")
    val storageEngineTask = ScalaEngineTask(
      traceRoot.canonicalAddress).toJvmTask // can do *5
    val traceTask = TraceCollectorTask(
      RunParams(
        GenericClusterParams(
          traceRoot.canonicalAddress,
          10,
          1,
          10
        ),
        "getRangeQuery"
      )
    ).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(storageEngineTask :: traceTask :: Nil)
  }
  
  def startScadrTraceCollector: Unit = {
    val traceTask = ScadrTraceCollectorTask(
      RunParams(
        scadrClusterParams,
        "mySubscriptions"
      )
    ).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startThoughtstreamTraceCollector: Unit = {
    val traceTasks = Array.fill(thoughtstreamRunParams.numTraceCollectors)(ThoughtstreamTraceCollectorTask(thoughtstreamRunParams).toJvmTask)
    
    serviceScheduler !? RunExperimentRequest(traceTasks.toList)
  }

  def startOneThoughtstreamTraceCollector: Unit = {
    val traceTask = ThoughtstreamTraceCollectorTask(thoughtstreamRunParams).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startLocalUserThoughtstreamTraceCollector: Unit = {
    val traceTasks = Array.fill(localUserThoughtstreamRunParams.numTraceCollectors)(ThoughtstreamTraceCollectorTask(localUserThoughtstreamRunParams).toJvmTask)
    
    serviceScheduler !? RunExperimentRequest(traceTasks.toList)
  }

  def startOneLocalUserThoughtstreamTraceCollector: Unit = {
    val traceTask = ThoughtstreamTraceCollectorTask(localUserThoughtstreamRunParams).toJvmTask
    
    serviceScheduler !? RunExperimentRequest(traceTask :: Nil)
  }

  def startScadrDataLoad: Unit = {
    val engineTask = ScalaEngineTask(traceRoot.canonicalAddress).toJvmTask
    val loaderTask = ScadrDataLoaderTask(scadrClusterParams).toJvmTask

    val storageEngines = Vector.fill(scadrClusterParams.numStorageNodes)(engineTask)
    val dataLoadTasks = Vector.fill(scadrClusterParams.numLoadClients)(loaderTask)

    serviceScheduler !? (RunExperimentRequest(storageEngines), 30 * 1000)
    serviceScheduler !? (RunExperimentRequest(dataLoadTasks), 30 * 1000)
}

  def startComradesDirector(add:Option[String] = None): Unit = {
    val clusterAddress = add.getOrElse(comradesRoot.canonicalAddress)
    val task = ComradesDirectorTask(
      clusterAddress,
      mesosMaster
    ).toJvmTask
    serviceScheduler !? RunExperimentRequest(task :: Nil)
  }

  def startScadrRain: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(rainJars,
        "radlab.rain.Benchmark",
        "rain.config.scadr.ramp.json" ::
        scadrWebServerList.canonicalAddress :: Nil,
        Map("dashboarddb" -> dashboardDb)) :: Nil)
  }

  def startGraditRain: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(rainJars,
		  "radlab.rain.Benchmark",
		  "rain.config.gradit.ramp.json" ::
		  graditWebServerList.canonicalAddress :: Nil,
		  Map("dashboarddb" -> dashboardDb)) :: Nil)
  }

  def startComradesRain: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(rainJars,
		  "radlab.rain.Benchmark",
		  "rain.config.comrades.demo.json" ::
		  comradesWebServerList.canonicalAddress :: Nil,
		  Map("dashboarddb" -> dashboardDb)) :: Nil)
  }

  def killTask(taskId: Int): Unit =
    serviceScheduler !? KillTaskRequest(taskId)

  /**
   * WARNING: deletes all data from all scads cluster
   */
  def resetScads: Unit = {
    // val namespaces = "users" :: "thoughts" :: "subscriptions" :: Nil
    // val delCmd = "rm -rf " + namespaces.map(ns => "/mnt/" + ns + "*").mkString(" ")
    // MesosEC2.slaves.pforeach(_ ! delCmd)

    scadrRoot.deleteRecursive
    graditRoot.deleteRecursive
    comradesRoot.deleteRecursive
  }

  def resetTracing: Unit = {
    traceRoot.data = "".getBytes
    traceRoot.deleteRecursive
  }

  def startIntKeyTest: Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(MesosEC2.classSource,
        "edu.berkeley.cs.radlab.demo.IntKeyScaleScheduler",
        "--name" :: "intkeyscaletest" ::
        "--mesosMaster" :: mesosMaster ::
        "--executor" :: javaExecutorPath ::
        "--cp" :: MesosEC2.classSource.map(safeUrl(_)).mkString("|") :: Nil) :: Nil)
  }

  def startRepTest(numKeys: Int): Unit = {
    serviceScheduler !? RunExperimentRequest(
      JvmMainTask(MesosEC2.classSource,
        "edu.berkeley.cs.radlab.demo.RepTestScheduler",
        "--name" :: "reptest" ::
        "--mesosMaster" :: mesosMaster ::
        "--executor" :: javaExecutorPath ::
        "--cp" :: MesosEC2.classSource.map(safeUrl(_)).mkString("|") ::
        "--numKeys" :: numKeys.toString :: Nil) :: Nil)
  }

  def loadTwitterSpam(): Unit = {
    val numServers = 16
    twitterSpamRoot.children.foreach(_.deleteRecursive)
    val cluster = new ExperimentalScadsCluster(twitterSpamRoot)

    serviceScheduler !? RunExperimentRequest(
      List.fill(numServers)(ScalaEngineTask(twitterSpamRoot.canonicalAddress).toJvmTask))
    cluster.blockUntilReady(numServers)

    serviceScheduler !? RunExperimentRequest(
      LoadJsonToScadsTask("http://cs.berkeley.edu/~marmbrus/tmp/labeledTweets.avro", twitterSpamRoot.canonicalAddress).toJvmTask :: Nil)
  }

  def authorizeUsers: Unit = {
    val keys =
      "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCfH8CkLrCIOxJAkFubG1ehQEdu1OfOUqaMxiTQ7g/X0fXclXRzqwoBFBL33t0FGVxkPVxolwAaZEQTIg6hkGZuzLlPiuq1ortkMx3wGxU9/YBr6JzSZb+kB1OEG/LOWiXH+i5IJbKptW+6B527niXCAgo8Idlf5PNBqcdI+CrvaX+oqQX6K2T5EDxoJVOtgRHbS/2YbtGhwknskyCcvOnOcwjcRUGawmVK7QYavyuO+//SOK+0sIjTSSwTAVceKbQl8XVlPL7IJHKE6/JwEF2+6+eMdflg9A8qAm3g0rE8qfUGdJLN1hpJNdP/UCP1v091h4C88lqqtwbekrS817ar stephentu@ibanez" ::
      "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAnOr61V36/yp1nGRfMZHxzFr1GUom/uQgQS5xdMQ3A56xqfWbhNpNTGQSOpKc3u1jfc77KouG8v0bPSssaiBIDIqVqVRWWACUg6j4xk5oN0lSm22LWJ0OnFvbPbsZlJOb9t+gIe2/yjlbJsyH5/mpIqJBTASOtXugYUIP3jIfA438ZiObpEYuL3kCiBDhEz4w6WbTaXr0K/bRxQoZFGJem+IH26bfeEP8Y12ygdgwh0EAKErv1bbULV7WC92F+5nSU1eGbvCKbhqxIUzxh7ZCRXdUyGcpDOfVL2MOUxNch3AKjE+Z5TVI8fv1md7ILK4dE95oJTUiWv9IUpAUEabM4Q== kristal.curtis@gmail.com" ::
      "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAqxXSYqw5Cshu9EvHCzRiu/1lSO6n6hqTmM7NwITK7Q5a0pn7Hg/ZzykuWpcrCDKjRadJlP+FrWWPdizEgOtgwmHs9LWrf0DrtLDNroRsgqyii/rD4+kAMgMP6PWxoRRUklo8Vqgt4TFwA/bDbKFHtyvnPGOxBiapnhrdWL5HKG1nIChrN2iLLq4ymnGd2N0pJW+Fwz8E/D4Mxk7poKSuyfyEcAynhAeLcCvx54t/p8JalahHHco+TGFEChp3gZ2c+Eov3KNZgtCGcQeIphVoRI5M+Li5adaVwD5Y3mmJ3yBBiS6rh4qN0QS9RecOH+oYcAOQWAm000q1UdfHfESKvQ== rean@eecs.berkeley.edu" ::
      "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAIEA4HH/XGUrla7FpONvVvHZAQ5XtY5hfZq3YuIICholyKfarp4O/sCT0EFjyO97IQILgM7HjDooGJKpexYL61JmnOWReRamsRCP1FKFiM03KofLpBvxllO8QbalSnjLfV9oVqTFDkwnRHQVbaN83FND2dIkEntyaX3U//8cIi3mFJ8= jtma@ironic.ucsd.edu" :: Nil

    MesosEC2.masters.pforeach(_.appendFile(new File("/root/.ssh/authorized_keys"), keys.mkString("\n")))
  }
}
