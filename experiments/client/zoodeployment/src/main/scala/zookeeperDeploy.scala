package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift.{Record,RecordSet}
import edu.berkeley.cs.scads.model._
import edu.berkeley.cs.scads.placement._
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol, XtBinaryProtocol}

import org.apache.zookeeper._

import scala.collection.jcl.Conversions

case class RebalanceInvariantViolationException(msg:String) extends Exception
case class InconsistentReplicationException(msg:String) extends Exception
case class NonDivisibleReplicaNumberException(x: Int, y: Int) extends Exception

class ClusterDeploy(val zookeeperHost:String, val zookeeperPort:Int, val numReplicas:Int) extends Watcher {
    val zoo = new ZooKeeper(zookeeperHost+":"+zookeeperPort, 3000, this)
    val cluster = new ZooKeptCluster(zookeeperHost+":"+zookeeperPort)
    val logger = Logger.getLogger("clusterDeploy")

    def rebalance():Unit = {
        /* Step 1: Ask zookeeper for all storage engine nodes */
        val logicalBuckets = getLogicalBuckets()
        val knownNs = getAllNamespaces()

        var bucketMap = Map[String,List[Int]]()
        knownNs.foreach( (ns) => {
            var bucketList = List[Int]()
            logicalBuckets.foreach( (partition) => {
                var bucketCount = 0 
                var seenOne = false
                partition.foreach( (smachine) => {
                    logger.debug("Looking at node: " + smachine.host)
                    val policies = Conversions.convertList(smachine.useConnection[java.util.List[RecordSet]]((c) => c.get_responsibility_policy(ns)))
                    if ( policies.size == 1 ) {
                        logger.debug("DP is valid...")
                        val count = smachine.totalCount(ns)
                        if ( !seenOne ) {
                            bucketCount = count
                        } else if ( bucketCount != count ) {
                           throw new InconsistentReplicationException("Found node " + smachine + " that has inconsistent data with the partition: bucketCount: " + bucketCount + " count: " + count) 
                        }
                        seenOne = true
                        logger.debug("Storage node: " + smachine)
                        logger.debug("Count: " + count)
                    } else if ( policies.size == 0 ) {
                        logger.debug("No Data Placement for " + smachine)
                        if ( bucketCount != 0 ) {
                           throw new InconsistentReplicationException("Found node " + smachine + " that has no data, but the partition does") 
                        }
                    } else {
                        logger.debug("Found a node with mulitple policies per namespace...")
                        throw new RebalanceInvariantViolationException(">1 policy per NS")
                    }
                })
                bucketList = bucketList ::: List(bucketCount)
            })
            bucketMap += ( ns -> bucketList ) 
        })

        println("LOGICAL BUCKETS: " + logicalBuckets)
        println("BUCKET MAP: " + bucketMap)
    }

    def getLogicalBuckets():List[List[NamedStorageMachine]] = {
        val uris = Conversions.convertList(zoo.getChildren("/scads/servers",true)).toList
        logger.debug("URIS: " + uris)
        val storageMachines = uris.map(uri => {
            val info = uri.split(":")
            new NamedStorageMachine(info(0),info(1).toInt)
        })

        var logicalBuckets = List[List[NamedStorageMachine]]()

        // We are enforcing the constraint that the number of storage 
        // nodes must be divisble by the number of replicas
        if ( numReplicas <= 0 || (storageMachines.size % numReplicas) != 0 ) {
            throw new NonDivisibleReplicaNumberException(storageMachines.size, numReplicas)
        }

        val numBuckets = storageMachines.size / numReplicas
        val storageMachineList = storageMachines
        var last = 0
        while ( logicalBuckets.size < numBuckets ) {
            logicalBuckets = logicalBuckets ::: List(storageMachineList.slice(last,last+numReplicas))
            last = last + numReplicas
        }

        logger.debug("LOGICAL BUCKETS: " + logicalBuckets)

        logicalBuckets
    }

    def getAllNamespaces():List[String] = {
        Conversions.convertList(zoo.getChildren("/scads/namespaces",false)).toList 
    }



    override def process(event:WatchedEvent):Unit = {
        logger.debug("zookeper event: " + event)
        // do nothing for now
    }

}
