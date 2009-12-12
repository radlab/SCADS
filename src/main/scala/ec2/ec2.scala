package deploylib.ec2

import deploylib.xresults._
import deploylib.runit._

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._
import org.apache.log4j.Logger
import java.io.File

import scala.collection.jcl.Conversions._

object EC2 {
	protected val logger = Logger.getLogger("deploylib.ec2")

	private val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  private val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")

  var keyName = System.getenv("AWS_KEY_NAME")
  var keyPath = System.getenv("AWS_KEY_PATH")

	private val config = new AmazonEC2Config()

  if(System.getenv("EC2_URL") != null)
  	config.setServiceURL(System.getenv("EC2_URL"))

  val client = new AmazonEC2Client(accessKeyId, secretAccessKey, config)
	val instances = new scala.collection.mutable.HashMap[String, RunningInstance]
	var lastUpdate = 0L

	update()

	def update():Unit = {
		synchronized {
			if(System.currentTimeMillis() - lastUpdate < 10000)
				logger.debug("Skipping ec2 update since it was done less than 10 seconds ago")
			else {
				val result = client.describeInstances(new DescribeInstancesRequest()).getDescribeInstancesResult()
				result.getReservation.foreach((r) => {
					r.getRunningInstance.foreach((i) => {
						instances.put(i.getInstanceId, i)
					})
				})
				lastUpdate = System.currentTimeMillis()
			}
		}
	}

	def activeInstances: List[EC2Instance] = {
		update()
		instances.keys.map(new EC2Instance(_)).filter(_.instanceState equals "running").toList
	}

	def myInstances: List[EC2Instance] = activeInstances.filter(_.keyName equals keyName)

	def runInstance(): EC2Instance =
		runInstances("ami-e7a2448e", 1, 1, "marmbrus", "m1.small", "us-east-1a")(0)

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

		val retInstances =
			synchronized {
				result.getReservation().getRunningInstance().map(ri => {
					instances.put(ri.getInstanceId, ri)
					new EC2Instance(ri.getInstanceId())
				})
			}

		val statsCollection = Future {
			logger.debug("Starting thread to collect ec2instance stats")
			retInstances.foreach(r => {
				r.blockUntilRunning
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
			logger.debug("Ec2Instance data stored to xmldb")
		}

		return retInstances
	}
}

class EC2Instance(val instanceId: String) extends RemoteMachine with RunitManager {
	lazy val hostname: String = getHostname()
	val username: String = "root"
	val privateKey: File = new File("/Users/marmbrus/.ec2/amazon/marmbrus.key")
	val rootDirectory: File = new File("/mnt/")
	val runitBinaryPath:File = new File("/usr/bin")
	val javaCmd:File = new File("/usr/bin/java")

	def halt: Unit =
		executeCommand("halt")

	def currentState: RunningInstance =
		EC2.instances(instanceId)

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
		EC2.instances(instanceId).getInstanceState.getName()

	def getHostname(): String = {
		blockUntilRunning()
		publicDnsName
	}

	def blockUntilRunning():Unit = {
		while(instanceState equals "pending"){
			logger.info("Waiting for instance " + this)
			Thread.sleep(10000)
			EC2.update()
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

  override def equals(other: Any): Boolean = other match {
    case that: EC2Instance => instanceId.equals(that.instanceId)
    case _ => false
  }

  override def hashCode: Int = instanceId.hashCode

	override def toString(): String = "<EC2Instance " + instanceId + ">"
}
