package edu.berkeley.cs
package radlab
package demo

import avro.marker._
import avro.runtime._

import scads.perf.ExperimentalScadsCluster

import scads.comm._
import scads.storage.ScalaEngineTask
import deploylib.mesos._
import deploylib.ec2._
import twitterspam.AvroHttpFile

import net.lag.logging.Logger

import java.io.File

object DemoConfig {
  protected val logger = Logger()
  val zone = "us-east-1a"
  val appServerCapacity = 10 /* req/sec */
  
  def twitterSpamRoot = zooKeeperRoot.getOrCreate("twitterSpam")

  val javaExecutorPath = "/usr/local/mesos/frameworks/deploylib/java_executor"

  //TODO: Add other ZooKeeper
  val zooKeeperRoot = ZooKeeperNode("zk://ec2-50-16-2-36.compute-1.amazonaws.com,ec2-174-129-105-138.compute-1.amazonaws.com/demo")

  val mesosMasterNode = zooKeeperRoot.getOrCreate("mesosMaster")
  def mesosMaster = new String(mesosMasterNode.data)

  def serviceSchedulerNode = zooKeeperRoot.getOrCreate("serviceScheduler")
  def serviceScheduler = classOf[RemoteActor].newInstance.parse(serviceSchedulerNode.data)

  /* SCADr */
  def scadrRoot =  zooKeeperRoot.getOrCreate("apps/scadr")
  def scadrWebServerList = scadrRoot.getOrCreate("webServerList")
  val scadrWarFile = new File("piql/scadr/src/main/rails/rails.war")
  def scadrWar =
    if(scadrWarFile.exists)
      S3CachedJar(S3Cache.getCacheUrl(scadrWarFile))
    else {
      logger.info("Using cached scadr war file.")
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-marmbrus/3a7c8abd9da8ba27e4bd822135179a6b")
    }

  /* gRADit */
  def graditRoot =  zooKeeperRoot.getOrCreate("apps/gradit")
  def graditWebServerList = graditRoot.getOrCreate("webServerList")
  val graditWarFile = new File("piql/gradit/src/main/rails/rails.war")
  def graditWar =
    if(graditWarFile.exists)
      S3CachedJar(S3Cache.getCacheUrl(graditWarFile))
    else {
      logger.info("Using cached gradit war file.")
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-marmbrus/5a65ddddab94db7bfa7cdf5e9914c47c")
    }

  /* comRADes */
  def comradesRoot =  zooKeeperRoot.getOrCreate("apps/comrades")
  def comradesWebServerList = comradesRoot.getOrCreate("webServerList")
  val comradesWarFile = new File("piql/comrades/src/main/rails/rails.war")
  def comradesWar =
    if(comradesWarFile.exists)
      S3CachedJar(S3Cache.getCacheUrl(comradesWarFile))
    else {
      logger.info("Using cached comrades war file.")
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-marmbrus/ab396cd6bc6c25e3496590c73ff816f4")
    }

  val jdbcDriver = classOf[com.mysql.jdbc.Driver]
  val dashboardDb = "jdbc:mysql://dev-mini-demosql.cwppbyvyquau.us-east-1.rds.amazonaws.com:3306/radlabmetrics?user=radlab_dev&password=randyAndDavelab"

  def rainJars = {
    val rainLocation  = new File("../rain-workload-toolkit")
    val workLoadDir = new File(rainLocation, "workloads")
    val rainJar = new File(rainLocation, "rain.jar")
    val scadrJar = new File(workLoadDir, "scadr.jar")
    val graditJar = new File(workLoadDir, "gradit.jar")

    if(rainJar.exists && scadrJar.exists && graditJar.exists) {
      logger.info("Using local jars")
      S3CachedJar(S3Cache.getCacheUrl(rainJar.getCanonicalPath)) ::
      S3CachedJar(S3Cache.getCacheUrl(scadrJar.getCanonicalPath)) :: 
      S3CachedJar(S3Cache.getCacheUrl(graditJar.getCanonicalPath)) :: Nil
    }
    else {
      logger.info("Using cached S3 jars")
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/f2f74da753d224836fedfd56c496c50a") ::
      S3CachedJar("http://s3.amazonaws.com/deploylibCache-rean/3971dfa23416db1b74d47af9b9d3301d") :: Nil
    }
  }

  implicit def classSource = MesosEC2.classSource

  protected def toServerList(node: ZooKeeperProxy#ZooKeeperNode) = {
    val servers = new String(scadrWebServerList.data).split("\n")
    servers.zipWithIndex.map {
      case (s: String, i: Int) => <a href={"http://%s:8080/".format(s)} target="_blank">{i}</a>
    }
  }

  def toHtml: scala.xml.NodeSeq = {
    <div>RADLab Demo Setup: <a href={"http://" + serviceScheduler.host + ":8080"} target="_blank">Mesos Master</a><br/> 
      Scadr Servers: {toServerList(scadrWebServerList)}
    </div>
  }

  /**
  e.g val namespaces = Map("users" -> classOf[edu.berkeley.cs.scads.piql.scadr.User],
	       "thoughts" -> classOf[edu.berkeley.cs.scads.piql.scadr.Thought],
	       "subscriptions" -> classOf[edu.berkeley.cs.scads.piql.scadr.Subscription])
  */
  def initScadrCluster(clusterAddress:String):Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    logger.info("Adding servers to cluster for each namespace")
    val namespaces = Map("users" -> classOf[edu.berkeley.cs.scads.piql.scadr.User],
  	       "thoughts" -> classOf[edu.berkeley.cs.scads.piql.scadr.Thought],
  	       "subscriptions" -> classOf[edu.berkeley.cs.scads.piql.scadr.Subscription])
    serviceScheduler !? RunExperimentRequest(namespaces.keys.toList.map(key => ScalaEngineTask(clusterAddress = cluster.root.canonicalAddress, name = Option(key + "!node0")).toJvmTask ))
    
    cluster.blockUntilReady(namespaces.size)
    logger.info("Creating the namespaces")
    namespaces.foreach {
      case (name, entityType) => {
	      logger.info("Creating namespace %s", name)
	      val entity = entityType.newInstance
	      val (keySchema, valueSchema) = (entity.key.getSchema, entity.value.getSchema) //entity match {case e:AvroPair => (e.key.getSchema, e.value.getSchema) }
	      val initialPartitions = (None, cluster.getAvailableServers(name)) :: Nil
	      cluster.createNamespace(name, keySchema, valueSchema, initialPartitions)
      }
    }
    startScadrDirector()
  }

  import edu.berkeley.cs.scads.piql.gradit._
  def initGraditCluster(clusterAddress:String):Unit = {
    import edu.berkeley.cs.scads.piql.SimpleExecutor

    val clusterRoot = ZooKeeperNode(clusterAddress)
    val cluster = new ExperimentalScadsCluster(clusterRoot)

    val dummyValueSchema = """{"type":"record","name":"DummyValue","namespace":"","fields":[{"name":"b","type":"boolean"}]}"""
    val indexNamespaceTuples = List( // tuples of (keySchemaString, valueSchemaString, nameString)
      ("""{"type":"record","name":"(user2)Key","namespace":"","fields":[{"name":"user2","type":"string"},{"name":"timestamp","type":"string"},{"name":"user1","type":"string"}]}""", dummyValueSchema, "challenges_(user2)"),
      ("""{"type":"record","name":"(score)Key","namespace":"","fields":[{"name":"score","type":"int"},{"name":"login","type":"string"},{"name":"gameid","type":"int"}]}""", dummyValueSchema, "gameplayers_(score)"),
      ("""{"type":"record","name":"(gameid)Key","namespace":"","fields":[{"name":"gameid","type":"int"},{"name":"login","type":"string"}]}""", dummyValueSchema, "gameplayers_(gameid)"),
      ("""{"type":"record","name":"(user1)Key","namespace":"","fields":[{"name":"user1","type":"string"},{"name":"timestamp","type":"string"},{"name":"user2","type":"string"}]}""", dummyValueSchema, "challenges_(user1)"),
      ("""{"type":"record","name":"(login)Key","namespace":"","fields":[{"name":"login","type":"string"},{"name":"name","type":"string"}]}""", dummyValueSchema, "wordlists_(login)"),
      ("""{"type":"record","name":"(wordlist)Key","namespace":"","fields":[{"name":"wordlist","type":"string"},{"name":"wordid","type":"int"}]}""", dummyValueSchema, "words_(wordlist)"),
      ("""{"type":"record","name":"(game2)Key","namespace":"","fields":[{"name":"game2","type":"int"},{"name":"timestamp","type":"string"},{"name":"user1","type":"string"},{"name":"user2","type":"string"}]}""", dummyValueSchema, "challenges_(game2)"),
      ("""{"type":"record","name":"(word)Key","namespace":"","fields":[{"name":"word","type":"string"},{"name":"wordid","type":"int"}]}""", dummyValueSchema, "words_(word)"),
      ("""{"type":"record","name":"(game1)Key","namespace":"","fields":[{"name":"game1","type":"int"},{"name":"timestamp","type":"string"},{"name":"user1","type":"string"},{"name":"user2","type":"string"}]}""", dummyValueSchema, "challenges_(game1)")
    )

    logger.info("Adding servers to cluster for each namespace")
    val namespaces = Map("words" -> classOf[edu.berkeley.cs.scads.piql.gradit.Word],
  	       "books" -> classOf[edu.berkeley.cs.scads.piql.gradit.Book],
  	       "wordcontexts" -> classOf[edu.berkeley.cs.scads.piql.gradit.WordContext],
  	       "wordlists" -> classOf[edu.berkeley.cs.scads.piql.gradit.WordList],
  	       "wordlistwords" -> classOf[edu.berkeley.cs.scads.piql.gradit.WordListWord],
  	       "games" -> classOf[edu.berkeley.cs.scads.piql.gradit.Game],
  	       "gameplayers" -> classOf[edu.berkeley.cs.scads.piql.gradit.GamePlayer],
  	       "users" -> classOf[edu.berkeley.cs.scads.piql.gradit.User],
  	       "challenges" -> classOf[edu.berkeley.cs.scads.piql.gradit.Challenge])
    serviceScheduler !? RunExperimentRequest(namespaces.keys.toList.map(key => ScalaEngineTask(clusterAddress = cluster.root.canonicalAddress, name = Option(key + "!node0")).toJvmTask ))
    serviceScheduler !? RunExperimentRequest(indexNamespaceTuples.map(entry => ScalaEngineTask(clusterAddress = cluster.root.canonicalAddress, name = Option(entry._3 + "!node0")).toJvmTask ))

    cluster.blockUntilReady(namespaces.size + indexNamespaceTuples.size)
    logger.info("Creating the namespaces")
    namespaces.foreach {
      case (name, entityType) => {
	      logger.info("Creating namespace %s", name)
	      val entity = entityType.newInstance
	      val (keySchema, valueSchema) = (entity.key.getSchema, entity.value.getSchema)
	      val initialPartitions = (None, cluster.getAvailableServers(name)) :: Nil
	      cluster.createNamespace(name, keySchema, valueSchema, initialPartitions)
      }
    }
    logger.info("Creating the index namespaces")
    indexNamespaceTuples.foreach {
      case (keyStr, valStr, name) => {
        logger.info("Creating namespace %s", name)
        val (keySchema, valueSchema) = (org.apache.avro.Schema.parse(keyStr), org.apache.avro.Schema.parse(valStr))
        assert(cluster.getAvailableServers(name).size == 1, "Namespace "+name+" has wrong number of partitions")
        val initialPartitions = (None, cluster.getAvailableServers(name)) :: Nil
        cluster.createNamespace(name, keySchema, valueSchema, initialPartitions)
        val originNsAndIndexName = name.split("_")
        cluster.root("namespaces")(originNsAndIndexName.head).getOrCreate("indexes").createChild(originNsAndIndexName(1), Array.empty, org.apache.zookeeper.CreateMode.PERSISTENT)
      }
    }

    logger.info("Populating gRADit with data")
    loadGraditData(new GraditClient(cluster, new SimpleExecutor))

    startGraditDirector()
  }

  def loadGraditData(client: GraditClient): Unit = {
    client.wordlists ++= AvroHttpFile[WordList]("http://gradit.s3.amazonaws.com/wordlists.avro")
    client.wordlistwords ++= AvroHttpFile[WordListWord]("http://gradit.s3.amazonaws.com/wordlistwords.avro")
    client.words ++= AvroHttpFile[Word]("http://gradit.s3.amazonaws.com/words.avro")
    client.wordcontexts ++= AvroHttpFile[WordContext]("http://gradit.s3.amazonaws.com/wordcontexts.avro")
    client.users ++= AvroHttpFile[User]("http://gradit.s3.amazonaws.com/users.avro")
  }
}
