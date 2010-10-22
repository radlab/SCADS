package deploylib.ec2

import deploylib._
import deploylib.runit._

import com.amazonaws.ec2._
import com.amazonaws.ec2.model._
import net.lag.logging.Logger
import java.io.File

import scala.collection.JavaConversions._
import scala.collection.immutable.TreeHashMap

/**
 * Provides static methds for interacting with instances running on amazons EC2.
 * It caches all data received from the EC2 api and updates it only when accessing a volitile field (such as instanceState) or when an update is manually requested.
 * Additionally, it will only make an update call to EC2 no more often than every 10 seconds.
 * This means it is safe to make many concurrent calls to the static methods or instance methods of a specific EC2Instance concurrently from many threads with out fear of overloading amazons api.
 */
object EC2Instance  extends AWSConnection {
	protected val logger = Logger()

  var keyName = System.getenv("AWS_KEY_NAME")

	private val config = new AmazonEC2Config()

  if(System.getenv("EC2_URL") != null)
  	config.setServiceURL(System.getenv("EC2_URL"))

  protected val client = new AmazonEC2Client(accessKeyId, secretAccessKey, config)
	var instanceData:Map[String, RunningInstance] = Map[String, RunningInstance]()
	protected val instances = new scala.collection.mutable.HashMap[String, EC2Instance]
	protected var lastUpdate = 0L

	/**
	 * Update the metadata for all ec2 instances.
	 * Safe to call repetedly, but will only actually update once ever 10 seconds.
   */
	def update():Unit = {
		synchronized {
			if(System.currentTimeMillis() - lastUpdate < 10000)
				logger.debug("Skipping ec2 update since it was done less than 10 seconds ago")
			else {
				val result = client.describeInstances(new DescribeInstancesRequest()).getDescribeInstancesResult()
				instanceData = Map(result.getReservation.flatMap((r) => {
					r.getRunningInstance.map((i) => {
						(i.getInstanceId, i)
					})
				}):_*)
				lastUpdate = System.currentTimeMillis()
				logger.info("Updated EC2 instances state")
			}
		}
	}

	/**
	 * Gets an instance object by instanceId
	 */
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

	/**
	 * Returns a list of all instances currently in the running state
	 */
	def activeInstances: List[EC2Instance] = {
		update()
		instanceData.keys.map(getInstance).filter(_.instanceState equals "running").toList
	}

	/**
	 * Returns all instances that are configured to use the key specified in keyName
	 */
	def myInstances: List[EC2Instance] = activeInstances.filter(_.keyName equals keyName)

	/**
	 * Launch a single golden image instance with default configuration.
	 */
	def runInstance(): EC2Instance =
		runInstances(1)(0)

	/**
	 * Launches the specified number of golden image instances with the default configuration.
	 */
	def runInstances(num: Int): Seq[EC2Instance] =
		runInstances("ami-e7a2448e", num, num, keyName, "m1.small", "us-east-1a")

	/**
	 * Launches a set of instances with the given parameters
	 */
	def runInstances(imageId: String, min: Int, max: Int, keyName: String, instanceType: String, location: String, userData: Option[String] = None): Seq[EC2Instance] = {
    val encoder = new sun.misc.BASE64Encoder
		val request = new RunInstancesRequest(
												imageId,                 // imageID
                        min,                     // minCount
                        max,                     // maxCount
                        keyName,                 // keyName
                        null,                    // securityGroup
                        userData.map(s => encoder.encode(s.getBytes)).orNull,         // userData
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

		})
		return retInstances
	}
}

/**
 * A specific RemoteMachine used to control a single EC2Instance.
 * Instances of this class can be obtained by instanceId from the static method EC2Instance.getInstance
 */
class EC2Instance protected (val instanceId: String) extends RemoteMachine with RunitManager {
	lazy val hostname: String = getHostname()
	val username: String = "root"
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

	/**
	 * Blocks the current thread until this instance is up and accepting ssh connections.
	 */
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

	/**
	 * Runs pre-experiment setup like recording instance details in the database and making needed directories
	 */
	def setup(): Unit = {
		if(ls(new File("/mnt")).filter(_.name equals "services").size == 0) {
			logger.debug("EC2Instance " + instanceId + " seen for the first time, configuring and storing xml")
			mkdir(new java.io.File("/mnt/services"))
		}
	}

	/**
	 * Custom upload method that copies a file once to S3 and then from there to any number of EC2Instances.
	 */
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
