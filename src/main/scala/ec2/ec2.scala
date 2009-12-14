package deploylib.ec2

import deploylib.xresults._
import deploylib.runit._

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._
import org.apache.log4j.Logger
import java.io.File

import scala.collection.jcl.Conversions._
import scala.collection.immutable.TreeHashMap

object EC2Instance  extends AWSConnection {
	protected val logger = Logger.getLogger("deploylib.ec2")

  var keyName = System.getenv("AWS_KEY_NAME")
  var keyPath = System.getenv("AWS_KEY_PATH")

	private val config = new AmazonEC2Config()

  if(System.getenv("EC2_URL") != null)
  	config.setServiceURL(System.getenv("EC2_URL"))

  protected val client = new AmazonEC2Client(accessKeyId, secretAccessKey, config)
	var instanceData:Map[String, RunningInstance] = new scala.collection.immutable.EmptyMap[String, RunningInstance]
	protected val instances = new scala.collection.mutable.HashMap[String, EC2Instance]
	protected var lastUpdate = 0L

	def update():Unit = {
		synchronized {
			if(System.currentTimeMillis() - lastUpdate < 10000)
				logger.debug("Skipping ec2 update since it was done less than 10 seconds ago")
			else {
				val result = client.describeInstances(new DescribeInstancesRequest()).getDescribeInstancesResult()
				instanceData = TreeHashMap(result.getReservation.flatMap((r) => {
					r.getRunningInstance.map((i) => {
						(i.getInstanceId, i)
					})
				}):_*)
				lastUpdate = System.currentTimeMillis()
				logger.info("Updated EC2 instances state")
			}
		}
	}

	def getInstance(instanceId: String): EC2Instance = {
		synchronized {
			instances.get(instanceId) match {
				case Some(inst) => inst
				case None => {
					val newInst = new EC2Instance(instanceId)
					instances.put(instanceId, newInst)
					return newInst
				}
			}
		}
	}

	def activeInstances: List[EC2Instance] = {
		update()
		instanceData.keys.map(getInstance).filter(_.instanceState equals "running").toList
	}

	def myInstances: List[EC2Instance] = activeInstances.filter(_.keyName equals keyName)

	def runInstance(): EC2Instance =
		runInstances(1)(0)

	def runInstances(num: Int): Seq[EC2Instance] =
		runInstances("ami-e7a2448e", num, num, keyName, "m1.small", "us-east-1a")

	def runInstances(imageId: String, min: Int, max: Int, keyName: String, instanceType: String, location: String): Seq[EC2Instance] = {
		val request = new RunInstancesRequest(
												imageId,                 // imageID
                        min,                     // minCount
                        max,                     // maxCount
                        keyName,                 // keyName
                        null,                    // securityGroup
                        null,                    // userData
                        instanceType,            // instanceType
                        new Placement(location), // placement
                        null,                    // kernelId
                        null,                    // ramdiskId
                        null,                    // blockDeviceMapping
                        null)                    // monitoring

		val result = client.runInstances(request).getRunInstancesResult()

		synchronized {
			instanceData ++= result.getReservation().getRunningInstance().map(ri => (ri.getInstanceId, ri))
		}

		val retInstances = result.getReservation().getRunningInstance().map(ri => getInstance(ri.getInstanceId))

		retInstances.foreach(r => {
			r.blockUntilRunning

			r.mkdir(new java.io.File("/mnt/services"))

			XResult.storeUnrelatedXml(
				<instance id={r.instanceId}>
					<imageId>{r.imageId}</imageId>
					<publicDnsName>{r.publicDnsName}</publicDnsName>
					<privateDnsName>{r.privateDnsName}</privateDnsName>
					<keyName>{r.keyName}</keyName>
					<instanceType>{r.instanceType}</instanceType>
					<availabilityZone>{r.availabilityZone}</availabilityZone>
				</instance>
			)
		})
		return retInstances
	}
}

class EC2Instance protected (val instanceId: String) extends RemoteMachine with RunitManager {
	lazy val hostname: String = getHostname()
	val username: String = "root"
	val privateKey: File = new File("/Users/marmbrus/.ec2/amazon/marmbrus.key")
	val rootDirectory: File = new File("/mnt/")
	val runitBinaryPath:File = new File("/usr/bin")
	val javaCmd:File = new File("/usr/bin/java")

	def halt: Unit =
		executeCommand("halt")

	def currentState: RunningInstance =
		EC2Instance.instanceData(instanceId)

	def imageId: String =
		currentState.getImageId()

	def publicDnsName: String =
		currentState.getPublicDnsName()

	def privateDnsName: String =
		currentState.getPrivateDnsName()

	def keyName: String =
		currentState.getKeyName()

	def instanceType: String =
		currentState.getInstanceType()

	def availabilityZone: String =
		currentState.getPlacement().getAvailabilityZone()

	def instanceState: String =
		EC2Instance.instanceData(instanceId).getInstanceState.getName()

	def getHostname(): String = {
		blockUntilRunning()
		publicDnsName
	}

	def blockUntilRunning():Unit = {
		while(instanceState equals "pending"){
			logger.info("Waiting for instance " + this)
			Thread.sleep(10000)
			EC2Instance.update()
		}

	 	var connected = false
		while(!connected) {
			try {
				val s = new java.net.Socket(publicDnsName, 22)
				connected = true
			}
			catch {
				case ce: java.net.ConnectException => {
					logger.info("SSH connection to " + publicDnsName + " failed, waiting 5 seconds")
				}
			}
			Thread.sleep(5000)
		}
	}

	override def upload(localFile: File, remoteDirectory: File): Unit = {
		val remoteFile = new File(remoteDirectory, localFile.getName)
		if(Util.md5(localFile) == md5(remoteFile))
			logger.debug("Not uploading " + localFile + " as the hashes match")
		else {
			val url = S3Cache.getCacheUrl(localFile)
			logger.debug("Getting file from cache: " + url)
			executeCommand("wget -O " + remoteFile + " " + url)
		}
	}

  override def equals(other: Any): Boolean = other match {
    case that: EC2Instance => instanceId.equals(that.instanceId)
    case _ => false
  }

  override def hashCode: Int = instanceId.hashCode

	override def toString(): String = "<EC2Instance " + instanceId + ">"
}
