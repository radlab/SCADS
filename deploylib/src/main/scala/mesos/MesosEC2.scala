package deploylib
package mesos

import ec2._
import config._

import edu.berkeley.cs.scads.comm._

import java.io.File
import java.net.InetAddress

object MesosEC2 extends ConfigurationActions {
  val rootDir = new File("/usr/local/mesos/frameworks/deploylib")

  def updateDeploylib: Unit = {
    val executorScript = Util.readFile(new File("deploylib/src/main/resources/java_executor"))
    slaves.pforeach(inst => {
      createDirectory(inst, rootDir)
      uploadFile(inst, new File("scads.jar"), rootDir)
      uploadFile(inst, new File("deploylib/src/main/resources/config"), rootDir)
      createFile(inst, new File(rootDir, "java_executor"), executorScript, "755")
    })
  }

  val masterTag = "mesosMaster"
  def slaves = EC2Instance.activeInstances.pfilterNot(_.tags contains masterTag)

  def masterCache = CachedValue(EC2Instance.activeInstances.pfilter(_.tags contains masterTag).head)
  def master = masterCache()

  def clusterUrl = "1@" + master.privateDnsName + ":5050"

  def restartSlaves: Unit = {
    slaves.pforeach(_ ! "service mesos-slave stop")
    slaves.pforeach(_ ! "service mesos-slave start")
  }

  def restartMaster: Unit = {
    master ! "service mesos-master stop"
    master ! "service mesos-master start"
  }

  def restart: Unit = {
    restartMaster
    restartSlaves
  }

  val defaultZone = "us-east-1a"
  def startMaster(zone: String = defaultZone):EC2Instance = {
    val ret = EC2Instance.runInstances(
        "ami-5a26d733",
        1,
        1,
        EC2Instance.keyName,
        "m1.large",
        zone,
        None).head
    ret.tags += masterTag
    restartMaster
    ret
  }

  def addSlaves(count: Int, zone: String = defaultZone): Seq[EC2Instance] = {
    val userData = try Some("url=" + clusterUrl) catch {
      case noMaster: java.util.NoSuchElementException =>
	logger.warning("No master found. Starting without userdata")
	None
    }

    EC2Instance.runInstances(
      "ami-5a26d733",
      count,
      count,
      EC2Instance.keyName,
      "m1.large",
      zone,
      userData)
  }

  def updateConf: Unit = {
    val conf = ("work_dir=/mnt" ::
      "log_dir=/mnt" ::
      "switch_user=0" ::
      "url="+clusterUrl :: Nil).mkString("\n")
    val conffile = new File("/usr/local/mesos/conf/mesos.conf")
    slaves.pforeach(_.createFile(conffile,conf))
  }

  //TODO: Doesn't handle non s3 cached jars
  def classSource: Seq[S3CachedJar] =
    if(System.getProperty("classsource") == null)
      pushJars.map(_.getName)
	.map(S3Cache.hashToUrl)
	.map(new S3CachedJar(_))
    else
      System.getProperty("classsource").split(":").map(S3CachedJar(_))

  def pushJars: Seq[String] = {
    val jarFile = new File("allJars")
    val jars = Util.readFile(jarFile).split("\n").map(new File(_))
    val (deploylib, otherJars) = jars.partition(_.getName contains "deploylib")
    val sortedJars = deploylib ++ otherJars

    logger.info("Starting Jar upload")
    sortedJars.map(S3Cache.getCacheUrl)
  }

  /**
   * Create a public key on the master if it doesn't exist
   * Then add that to the authorized key file all of slaves
   * TODO: Dedup keys
   */
  def authorizeMaster: Unit = {
    val getKeyCommand = "cat /root/.ssh/id_rsa.pub"
    val key = try (master !? getKeyCommand) catch {
      case u: UnknownResponse => {
	master ! "ssh-keygen -t rsa -f /root/.ssh/id_rsa -N \"\""
	master !? getKeyCommand
      }
    }

    slaves.pforeach(_.appendFile(new File("/root/.ssh/authorized_keys"), key))
  }
}
