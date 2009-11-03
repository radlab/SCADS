package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift._
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange}
import edu.berkeley.cs.scads.nodes.StorageNode

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol, XtBinaryProtocol}

case class RemotePortInUseException(msg: String) extends Exception
case class BlockingTriesExceededException(msg: String) extends Exception
case class NonDivisibleReplicaNumberException(x: Int, y: Int) extends Exception

trait RemoteHandleGetter[T] {

    private var xtrace_on = false
    val h: String
    val p: Int

    def getHandle():T = {
        var haveSNhandle = false
        var snclient:AnyRef = null  
        while (!haveSNhandle) {
            try {
                val transport = new TFramedTransport(new TSocket(h, p))
                val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
                snclient = getClient(protocol).asInstanceOf[AnyRef]
                transport.open()
                haveSNhandle = true
            } catch {
                case e: Exception => { println("don't have connection to placement server, waiting 1 second: " + e.getMessage); e.printStackTrace; Thread.sleep(1000) }
            }
        }
        snclient.asInstanceOf[T]
    }

    def getClient(protocol:TProtocol):T
}

trait Java2ScalaList { 
    
    implicit def javalist2scalalist[T](jlist: java.util.List[T]):List[T] = {
        val iter = jlist.iterator
        var rtn = List[T]()
        while ( iter.hasNext ) {
            rtn = rtn ::: List[T](iter.next)
        }
        rtn
    }

}

case class RemoteStorageNode(h:String, p: Int) extends RemoteHandleGetter[StorageEngine.Client] {
    override def getClient(protocol:TProtocol):StorageEngine.Client = new StorageEngine.Client(protocol)
}
    

case class RemoteDataPlacement(dataPlacementNode: Tuple2[RClusterNode,Int], logger: Logger, logicalBuckets: List[List[Tuple2[RClusterNode,Int]]]) extends RemoteDataPlacementProvider with Java2ScalaList with DataPlacementValidator {

    val host = dataPlacementNode._1.hostname
    val port = dataPlacementNode._2
    val xtrace_on = false
    val reverseMapping = generateReverseMapping()

    private def generateReverseMapping(): Map[Tuple2[String,Int],Int] = {
        var reverseMapping = Map[Tuple2[String,Int],Int]()
        for ( i <- 0 until logicalBuckets.size ) {
            logicalBuckets(i).foreach( (tuple) => reverseMapping += ( (tuple._1.hostname,tuple._2) -> i ) ) 
        }
        logger.debug("REVERSE MAPPING " + reverseMapping)
        reverseMapping
    }

    private def addNeverSeenNS(ns: String): Unit = {
        // for now, if we've NEVER seen an NS before, 
        // add all of its range to the first logical bucket
        val keyRange = new KeyRange(MinKey,MaxKey) // -infty to +infty
        assignRangeToLogicalPartition(0, ns, keyRange) // why isn't this implicit def??
    }

    override def lookup(ns: String): Map[StorageNode, KeyRange] = { 
        var ret = super.lookup(ns)
        if(ret.isEmpty) { addNeverSeenNS(ns); ret = super.lookup(ns) }
        ret 
    }   

    override def lookup(ns: String, node: StorageNode): KeyRange = { 
        var ret = super.lookup(ns, node)
        if(ret == KeyRange.EmptyRange) { addNeverSeenNS(ns); ret = super.lookup(ns, node) }
        ret 
    }   
    override def lookup(ns: String, key: Key):List[StorageNode] = { 
        var ret = super.lookup(ns, key)
        if(ret.isEmpty) { addNeverSeenNS(ns); ret = super.lookup(ns, key) }
        ret 
    }   
    override def lookup(ns: String, range: KeyRange): Map[StorageNode, KeyRange] = { 
        var ret = super.lookup(ns, range)
        if(ret.isEmpty) { addNeverSeenNS(ns); ret = super.lookup(ns, range) }
        ret 
    } 

    def rebalance():Unit = {
        val knownNs = space.keySet
        val handle = getDataPlacementHandle() 
        knownNs.foreach( (ns) => {
            
            logicalBuckets.foreach( (partition) => {
                partition.foreach( (tuple) => {
                    
                    logger.debug("Looking at node: " + tuple._1)
                    val rnode = new RemoteStorageNode(tuple._1.hostname, tuple._2)
                    val dp = handle.lookup_node(ns, tuple._1.hostname, tuple._2, tuple._2)
                    logger.debug("Returns DP: " + dp)
                    if ( isValidDataPlacement(dp) ) {
                        logger.debug("DP is valid...")
                        val rhandle = rnode.getHandle
                        val count = rhandle.count_set(ns, dp.rset)
                        dp.rset.range.setLimit(1)
                        val startR = rhandle.get_set(ns, dp.rset)
                        dp.rset.range.setOffset(count-1)
                        val endR = rhandle.get_set(ns, dp.rset)

                        logger.debug("Storage node: " + tuple._1)
                        logger.debug("Count: " + count)
                        logger.debug("Starting key: " + startR)
                        logger.debug("Ending Key: " + endR)
                    } else {
                        logger.debug("No Data Placement for " + tuple._1)
                    }

                })
            })




            //val dps = handle.lookup_namespace(ns) 
            //logger.debug("DPS FOR " + ns + " ARE: " + dps)
            //dps.foreach( (dp) => {
            //    logger.debug("DP IS" + dp)
            //    val lBucket = reverseMapping((dp.node,dp.thriftPort))
            //    logger.debug("REVERSE MAPPING IS " + lBucket)
            //    val rnode = new RemoteStorageNode(dp.node,dp.thriftPort)
            //    val rhandle = rnode.getHandle
            //    val count = rhandle.count_set(ns, dp.rset)
            //    logger.debug("LOGICAL BUCKET " + lBucket + " has " + count + " number of " + ns)
            //})
        })
    }

    def assignRangeToLogicalPartition(partition: Int, namespace:String, keyRange: KeyRange):Unit = {
        assert( 0 <= partition && partition <= logicalBuckets.size-1 )
        val dpclient = getDataPlacementHandle()
        logicalBuckets(partition).foreach( (tuple) => {
            val node = tuple._1
            val port = tuple._2
            val rs = new RecordSet(3,keyRangeToRangeSet(keyRange),null,null)
            val dp = new DataPlacement(node.hostname,port,port,rs)
            val ll = new java.util.LinkedList[DataPlacement]
            ll.add(dp)
            dpclient.add(namespace, ll)
            logger.debug("Added " + node + " to contain " + keyRange)
        })
    }

    def getDataPlacementHandle():KnobbedDataPlacementServer.Client = {
        var haveDPHandle = false
        var dpclient:KnobbedDataPlacementServer.Client = null
        while (!haveDPHandle) {
            try {
                val transport = new TFramedTransport(new TSocket(dataPlacementNode._1.hostname, dataPlacementNode._2))
                val protocol = if (xtrace_on) {new XtBinaryProtocol(transport)} else {new TBinaryProtocol(transport)}
                dpclient = new KnobbedDataPlacementServer.Client(protocol)
                transport.open()
                haveDPHandle = true
            } catch {
                case e: Exception => { logger.info("don't have connection to placement server, waiting 1 second: " + e.getMessage); e.printStackTrace; Thread.sleep(1000) }
            }
        }
        dpclient
    }

}

class ScadsDeploy(storageNodes: scala.collection.immutable.Map[RClusterNode,Int], dataPlacementNode: Tuple2[RClusterNode,Int], numReplicas: Int)  {

    // Default is there is only 1 replica (ie no replication)
    def this(storageNodes: scala.collection.immutable.Map[RClusterNode,Int], dataPlacementNode: Tuple2[RClusterNode,Int]) = this(storageNodes,dataPlacementNode,1)

    val logger = Logger.getLogger("ScadsDeploy")
    var debugLevel = Level.DEBUG
    var maxBlockingTries = 1000
    private val allNodes = storageNodes + dataPlacementNode
    private val logicalBuckets = buildLogicalBuckets()
    private val rdp = new RemoteDataPlacement(dataPlacementNode,logger,logicalBuckets)

    private def stopAllServices(): Unit = {
        allNodes.keySet.foreach(_.services.foreach(_.stop))
        allNodes.keySet.foreach(_.cleanServices)
    }

    def shutdown():Unit = {
        stopAllServices 
    }

    private def buildLogicalBuckets():List[List[Tuple2[RClusterNode,Int]]] = {
        var logicalBuckets = List[List[Tuple2[RClusterNode,Int]]]()

        // We are enforcing the constraint that the number of dataplacement
        // nodes must be divisble by the number of replicas
        if ( numReplicas <= 0 || (storageNodes.size % numReplicas) != 0 ) {
            throw new NonDivisibleReplicaNumberException(storageNodes.size, numReplicas)
        }

        val numNodesPerBucket = storageNodes.size / numReplicas
        val storageNodeList = storageNodes.keySet.toList
        var last = 0
        while ( logicalBuckets.size < numReplicas ) {
            logicalBuckets = logicalBuckets ::: List(storageNodeList.slice(last,last+numNodesPerBucket).map( (n) => (n, storageNodes(n) ) ))
            last = last + numNodesPerBucket
        }

        logger.debug("LOGICAL BUCKETS: " + logicalBuckets)

        logicalBuckets
    }

    def deploy():Unit = {



        // Set up the remote logger
        val remoteLogger = Logger.getLogger("deploylib.remoteMachine")
        remoteLogger.setLevel(debugLevel)

        // Iterate over all the storage + dataplacement nodes to
        // clean up any pre-existing services, so we start fresh
        stopAllServices

        // Give the services a chance to clean
        // TODO: we really need to have blocking command execution, this
        // is not the right way to do this
        Thread.sleep(1000)

        // Check to see if the port is open on the remote machine
        // for each node
        allNodes.keySet.foreach((node) => {
            if ( !node.isPortAvailableToListen(allNodes(node)) ) {
                val msg = "Port " + allNodes(node) + " is in use on " + node 
                logger.fatal(msg)
                throw new RemotePortInUseException(msg)
            }
        })

        // Start the storage engine service on all the storage nodes
        storageNodes.keySet.foreach( (rnode) => {
            val port = storageNodes(rnode) 
            // Setup runit on the node
            rnode.setupRunit
            val storageNodeService = new JavaService(
                "../../../scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.storage.JavaEngine","-p " +port)
            storageNodeService.action(rnode)
            rnode.services(0).watchLog
            rnode.services(0).start
            blockUntilRunning(rnode.services(0))
        })

        // Start up the data placement node
        val rnode = dataPlacementNode._1
        val port = dataPlacementNode._2
        val dataPlacementNodeService = new JavaService(
            "../../../placement/target/placement-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.placement.SimpleDataPlacementApp",port.toString)
        dataPlacementNodeService.action(rnode)
        rnode.services(0).watchLog
        rnode.services(0).start
        blockUntilRunning(rnode.services(0))

    }

    def rebalance():Unit = {
        rdp.rebalance
    }


    def getEnv():Environment = {
        implicit val env = new Environment
        env.placement = rdp
        env.session = new TrivialSession
        env.executor = new TrivialExecutor
        env
    }

    private def blockUntilRunning(runitService: Service):Unit = {
        var i = 0
        while( !runitService.status.trim.equals("run") ) {
            if ( i == maxBlockingTries ) {
                val msg = "Exceeded max blocking tries"
                logger.fatal(msg)
                throw new BlockingTriesExceededException(msg)
            }
            logger.info("got status '" + runitService.status + "', expecting 'run'")
            runitService.start // keep trying!
            Thread.sleep(1000);// try to mitigate busy-wait
            i += 1
        }
    }

    //-----------------------------------------------------------------------------------------------------------
    //-----------------------------------------------------------------------------------------------------------
    //-----------------------------------------------------------------------------------------------------------

    //private def placeEntities[T <: Field](entityPlacement: Array[Tuple3[T,T,RClusterNode]], namespace:String):Unit = {
    //    val dpclient = getDataPlacementHandle(dataPlacementNode._2,dataPlacementNode._1.hostname,false)
    //    entityPlacement.foreach( (tuple) => {
    //        val from = tuple._1
    //        val to = tuple._2
    //        val rnode = tuple._3
    //        
    //        val range = new RangeSet()
    //        range.setStart_key(from.serializeKey)
    //        range.setEnd_key(to.serializeKey)
    //        val rs = new RecordSet(3,range,null,null)
    //        val dp = new DataPlacement(rnode.hostname,storageNodes(rnode),storageNodes(rnode),rs)
    //        val ll = new java.util.LinkedList[DataPlacement]
    //        ll.add(dp)
    //        dpclient.add(namespace, ll)
    //    })

    //}

    //implicit def string2stringfield(s:String):StringField = { 
    //    val f = new StringField
    //    f.value = s
    //    f
    //}

    //implicit def int2intfield(i:Int): IntegerField = {
    //    val f = new IntegerField
    //    f.value = i
    //    f
    //}

    //def equalKeyPartitionUsers(usernames: List[String]):Unit = {
    //    assignEqualKeyPartition[StringField]( 
    //            usernames.map[StringField]( (s) => s ),
    //            (a,b)=>{ (a.value.compareTo(b.value))<0 },
    //            (a)=>{ val f = new StringField; f.value = a.value+"a"; f},
    //            getUserNamespace())
    //}

    //def equalKeyPartitionThoughts(thoughts: List[Int]):Unit = {
    //    assignEqualKeyPartition[IntegerField]( 
    //            thoughts.map[IntegerField]( (t) => t),
    //            (a,b)=>{ a.value < b.value },
    //            (a)=>{ val f = new IntegerField; f.value = a.value+1; f},
    //            getThoughtNamespace())
    //}

    //private def assignEqualKeyPartition[T <: Field](keylist:List[T], cmp:(T,T) => Boolean, dummyCallback:T => T, namespace:String ):Unit = {
    //    val n = storageNodes.size 
    //    val partitions = makeEqualKeyPartition[T](keylist,n,cmp,dummyCallback)
    //    logger.debug("Partitions " + partitions)
    //    var i = -1 
    //    placeEntities[T]( storageNodes.keySet.toList.map((p)=>{ i += 1; (partitions(i)._1,partitions(i)._2,p) }).toArray, namespace)
    //}

    //private def makeEqualKeyPartition[T <: Field](keylist:List[T], n:Int, cmp:(T,T) => Boolean, dummyCallback:T => T ): 
    //    List[Tuple2[T,T]] = {
    //    var keylistPP = 0

    //    val lower = Math.floor(keylist.length.toDouble/n.toDouble).toInt
    //    val upper = Math.ceil(keylist.length.toDouble/n.toDouble).toInt

    //    logger.debug("LOWER " + lower + " UPPER " + upper)

    //    if ( upper*(n-1) < keylist.size ) {
    //        val lowerMMDiff = Math.abs(keylist.size-lower*(n-1) - lower)
    //        val upperMMDiff = Math.abs(keylist.size-upper*(n-1) - upper)
    //        if ( lowerMMDiff < upperMMDiff ) {
    //            keylistPP = lower
    //        } else {
    //            keylistPP = upper
    //        }
    //    } else {
    //        keylistPP = lower
    //    }
    //    logger.debug("KEYLISTPP: " + keylistPP)

    //    val nkeylist = keylist.sort(cmp)

    //    var buckets = List[Tuple2[T,T]]()

    //    var bucketsSoFar = 0
    //    var countSoFar = 0
    //    var lastMaxRange = nkeylist(0)
    //    for ( i <- 0 until nkeylist.length+1 ) {
    //        if ( i == nkeylist.length ) {
    //            buckets += (lastMaxRange, dummyCallback(nkeylist(nkeylist.length-1)))
    //        } else if ( countSoFar == keylistPP && bucketsSoFar < (n-1) ) {
    //            var lmax = nkeylist(i)
    //            buckets += (lastMaxRange, lmax)  
    //            bucketsSoFar += 1
    //            lastMaxRange = lmax 
    //            countSoFar = 1
    //        } else {
    //            countSoFar += 1
    //        }
    //    }

    //    buckets
    //}


    //def placeThoughts(thoughtPlacement: Array[Tuple3[Int,Int,RClusterNode]]):Unit = {
    //    placeEntities[IntegerField](
    //        thoughtPlacement.map[Tuple3[IntegerField,IntegerField,RClusterNode]]( (tuple) => {
    //            val from = new IntegerField
    //            val to = new IntegerField
    //            from.value = tuple._1
    //            to.value = tuple._2
    //            val rnode = tuple._3
    //            ( from, to, rnode ) } ),
    //        getThoughtNamespace())
    //} 


    //def placeUsers(userPlacement: Array[Tuple3[String,String,RClusterNode]]):Unit = {
    //    placeEntities[StringField](
    //        userPlacement.map[Tuple3[StringField,StringField,RClusterNode]]( (tuple) => {
    //            val from = new StringField
    //            val to = new StringField
    //            from.value = tuple._1
    //            to.value = tuple._2
    //            val rnode = tuple._3
    //            ( from, to, rnode ) } ),
    //        getUserNamespace())
    //} 

    //private def getUserNamespace(): String = {
    //    //val user = new user()
    //    //user.namespace
    //    "ent_user"
    //}

    //private def getThoughtNamespace(): String = {
    //    //val thought = new thought()
    //    //thought.namespace
    //    "ent_thought"
    //}
}
