package edu.berkeley.cs.scads.deployment

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import deploylib._
import deploylib.rcluster._
import deploylib.configuration._
import deploylib.configuration.ValueConverstion._

import edu.berkeley.cs.scads.thrift.{Record,RecordSet,StorageNode}
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.model.{Environment,ZooKeptCluster,TrivialExecutor,TrivialSession}

import org.apache.zookeeper.{ZooKeeper,Watcher,WatchedEvent}

import scala.collection.jcl.Conversions

case class RebalanceInvariantViolationException(msg:String) extends Exception
case class InconsistentReplicationException(msg:String) extends Exception
case class NonDivisibleReplicaNumberException(x: Int, y: Int) extends Exception

class ClusterDeploy(val zookeeperHost:String, val zookeeperPort:Int, val numReplicas:Int) extends Watcher {
    val zoo = new ZooKeeper(zookeeperHost+":"+zookeeperPort, 3000, this)
    val cluster = new ZooKeptCluster(zookeeperHost+":"+zookeeperPort)
    val logger = Logger.getLogger("clusterDeploy")

    def getPartition(partition:Int):List[StorageNode] = {
        val logicalBuckets = getLogicalBuckets()
        assert( partition >= 0 && partition < logicalBuckets.size )
        logicalBuckets(partition)
    }

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
                    val policies = Conversions.convertList(smachine.useConnection((c) => c.get_responsibility_policy(ns)))
                    if ( policies.size == 1 ) {
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

        bucketMap.keySet.foreach( (ns) => {
            val bucketCount = bucketMap(ns)

            val numKeys = bucketCount.foldLeft(0)((b,a)=>b+a)
            val n = logicalBuckets.size

            var keylistPP = 0

            val lower = Math.floor(numKeys.toDouble/n.toDouble).toInt
            val upper = Math.ceil(numKeys.toDouble/n.toDouble).toInt

            logger.debug("LOWER " + lower + " UPPER " + upper)

            if ( upper*(n-1) < numKeys ) {
                val lowerMMDiff = Math.abs(numKeys-lower*(n-1) - lower)
                val upperMMDiff = Math.abs(numKeys-upper*(n-1) - upper)
                if ( lowerMMDiff < upperMMDiff ) {
                    keylistPP = lower
                } else {
                    keylistPP = upper
                }
            } else {
                keylistPP = lower
            }
            logger.debug("NS: " + ns + " gets KEYLISTPP: " + keylistPP)

            logger.debug("NAMESPACE: " + ns)
            logger.debug("------------------- BEGIN BEFORE DATA DUMP ------------------")
            dumpNodeData(ns)
            logger.debug("------------------- END BEFORE DATA DUMP ------------------")
            // move all the data first
            for ( i <- 0 until logicalBuckets.size-1 ) {
                for ( j <- 0 until logicalBuckets(i).size ) {
                    val smachine1 = logicalBuckets(i)(j)
                    val smachine2 = logicalBuckets(i+1)(j)

                    val curSize = smachine1.totalCount(ns)
                    val diff = Math.abs(curSize-keylistPP)

                    logger.debug("logicalBucket " + i + "," + j + " has count:" + curSize)

                    if ( curSize > keylistPP ) {
                        logger.debug("curSize>keylistPP case")
                        // need to move guys out!

                        val plus1 = smachine1.getNthRecord(ns,keylistPP)
                        logger.debug("KEYLISTPP+1-th elem :" + plus1)
                        val plus1keyval = plus1.key
                        logger.debug("KEYLISTPP+1-th key :" + plus1keyval)

                        val rset = Conversions.convertList(smachine1.useConnection((c) => c.get_responsibility_policy(ns))).toList.apply(0)
                        rset.range.setOffset(keylistPP) 
                        logger.debug("calling move()")
                        move(ns, rset, smachine1, smachine2) // spill the extra from A into B

                        rset.range.setEnd_key(plus1keyval) // update current node
                        smachine1.useConnection((c) => c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List(rset))))

                        // now need to update the next node
                        val nextRsetList = Conversions.convertList(smachine2.useConnection((c) => c.get_responsibility_policy(ns))).toList
                        val nextRset = nextRsetList.size match {
                            case 0 => ScadsDeployUtil.getRS(plus1keyval,null)
                            case 1 => { val tmp = nextRsetList(0); tmp.range.setStart_key(plus1keyval); tmp }
                            case _ => throw new IllegalStateException("Should not have a node with > 1 policy")
                        }

                        logger.debug("Next node gets RecordSet: " + nextRset)
                        smachine2.useConnection((c) => c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List(nextRset))))

                    } else if ( curSize < keylistPP ) {
                        logger.debug("curSize<keylistPP case")
                        logger.debug("need to move in " + diff + " keys!")

                        var keysMoved = 0
                        var sourceOffset = 1
                        var sourceMachine:DeployStorageNode = smachine2

                        while ( keysMoved < diff ) {
                            // need to move guys in from sourceMachine!

                            val nKeysBefore = sourceMachine.totalCount(ns)
                            if ( nKeysBefore != 0 ) {
                                logger.debug("Found keys in source node: " + sourceMachine)
                                val rset = Conversions.convertList(sourceMachine.useConnection((c) => c.get_responsibility_policy(ns))).toList.apply(0)
                                rset.range.setLimit(diff-keysMoved)
                                move(ns, rset, sourceMachine, smachine1)
                                val nKeysAfter = sourceMachine.totalCount(ns)
                                keysMoved += (nKeysBefore - nKeysAfter)
                                logger.debug("Moved " + (nKeysBefore-nKeysAfter) + " from " + sourceMachine + " to " + smachine1)

                                if ( nKeysAfter == 0 ) {
                                    logger.debug(sourceMachine + " is now empty!")
                                    // source is empty
                                    // Not sure if this is the right way to
                                    // clear a policy but here goes....
                                    sourceMachine.useConnection((c) => c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List())))
                                } else {
                                    // source still has keys- need to update the
                                    // start key of the guy
                                    logger.debug(sourceMachine + " still has keys")
                                    val firstKey = sourceMachine.getNthRecord(ns, 0)
                                    val sourceRset = Conversions.convertList(sourceMachine.useConnection((c) => c.get_responsibility_policy(ns))).toList.apply(0)
                                    sourceRset.range.setStart_key(firstKey.key)
                                    sourceMachine.useConnection((c) => c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List(sourceRset))))
                                }
                            }

                            if ( keysMoved < diff ) {
                                sourceOffset += 1
                                sourceMachine = logicalBuckets(i+sourceOffset)(j)
                            }

                        }

                        // set the RecordSet for smachine1
                        // step1: find the next node with some keys (possibly
                        // no such nodes exist)
                        var hasKey = sourceMachine.totalCount(ns) > 0
                        while ( !hasKey && (i+sourceOffset) < logicalBuckets.size-1 ) {
                            sourceOffset += 1
                            sourceMachine = logicalBuckets(i+sourceOffset)(j)
                            hasKey = sourceMachine.totalCount(ns) > 0
                        }

                        val thisRsetList = Conversions.convertList(smachine1.useConnection((c) => c.get_responsibility_policy(ns))).toList
                        val thisRset = thisRsetList.size match {
                            case 0 => ScadsDeployUtil.getAllRangeRecordSet()
                            case 1 => thisRsetList(0)
                            case _ => throw new IllegalStateException("Should not have a node with > 1 policy")
                        }

                        if ( hasKey ) {
                            val nPlus1Record = sourceMachine.getNthRecord(ns,0)
                            thisRset.range.setEnd_key(nPlus1Record.key)
                        } else {
                            thisRset.range.setEnd_key(null)
                        }

                        smachine1.useConnection((c) => c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List(thisRset))))
                    }
                }

            }


            logger.debug("NAMESPACE : " + ns)
            logger.debug("-------------------BEGIN AFTER DATA DUMP ------------------")
            dumpNodeData(ns)
            logger.debug("-------------------END AFTER DATA DUMP ------------------")


        })

    }
    
    implicit val stringSort:(String,String)=>Boolean = (e1,e2)=>(e1 compareTo e2)<0

    def getLogicalBuckets():List[List[DeployStorageNode]] = {
        val uris = Conversions.convertList(zoo.getChildren("/scads/servers",true)).toList.sort(stringSort)
        logger.debug("URIS: " + uris)
        val storageMachines = uris.map(uri => {
            val info = uri.split(":")
            new DeployStorageNode(info(0),info(1).toInt)
        })

        var logicalBuckets = List[List[DeployStorageNode]]()

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

    def getAllNamespaces():List[String] = zoo.exists("/scads/namespaces",false) match {
        case null => List()
        case _    => Conversions.convertList(zoo.getChildren("/scads/namespaces",false)).toList.sort(stringSort)
    }

    def getEnv():Environment = {
        implicit val env = new Environment
        env.placement = new ZooKeptCluster(zookeeperHost+":"+zookeeperPort) 
        env.session = new TrivialSession
        env.executor = new TrivialExecutor
        env
    }

    def move(namespace:String, range:RecordSet, src:StorageNode, dest:StorageNode):Boolean = {
        val isCopied = src.useConnection((c) => c.copy_set(namespace, range, dest.host+":"+dest.port))
        val isRemoved = isCopied match {
            case false => false
            case true  => src.useConnection((c) => c.remove_set(namespace, range))
        }
        isCopied && isRemoved
    }

    override def process(event:WatchedEvent):Unit = {
        logger.debug("zookeper event: " + event)
        // do nothing for now
    }

    private def dumpNodeData():Unit = {
        val knownNs = getAllNamespaces()
        knownNs.foreach( (ns) => dumpNodeData(ns) ) 
    }

    private def dumpNodeData(ns: String):Unit = {
        getLogicalBuckets().foreach( (partition) => {
            partition.foreach( (smachine) => {
                logger.debug("Looking at node: " + smachine)
                logger.debug("Data:" + smachine.totalSet(ns))
            }) 
        })
    }

}
