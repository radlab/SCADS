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
import edu.berkeley.cs.scads.keys.{MinKey,MaxKey,Key,KeyRange,StringKey}
import edu.berkeley.cs.scads.nodes._

import org.apache.thrift.transport.{TFramedTransport, TSocket}
import org.apache.thrift.protocol.{TProtocol,TBinaryProtocol, XtBinaryProtocol}

import scala.collection.jcl.Conversions

//case class RemotePortInUseException(msg: String) extends Exception
//case class BlockingTriesExceededException(msg: String) extends Exception
//case class NonDivisibleReplicaNumberException(x: Int, y: Int) extends Exception
//case class InconsistentReplicationException(msg: String) extends Exception

trait UnknownNSHandler {
    def handleUnknownNS(ns:String):Unit
}



class ScadsDeploy2(storageMachines: List[StorageMachine], dataPlacementMachine: DataPlacementMachine, numReplicas: Int) extends DataPlacementValidator {

    class DefaultUnknownNSHandler(logicalBuckets: List[List[StorageMachine]], var defaultPartition: Int) extends UnknownNSHandler {

        override def handleUnknownNS(ns:String):Unit = {
            logicalBuckets(defaultPartition).foreach( (sm) => {
                val dp = sm.getDataPlacement(ScadsDeployUtil.getAllRangeRecordSet)
                dataPlacementMachine.useConnection( (c) => c.add(ns, ListConversions.scala2JavaList(List(dp))) )
            })
        }

    }

    def this(storageMachines: List[StorageMachine], dataPlacementMachine: DataPlacementMachine) = this(storageMachines,dataPlacementMachine,1)

    val logger = Logger.getLogger("ScadsDeploy")
    var debugLevel = Level.DEBUG


    private val logicalBuckets = buildLogicalBuckets()

    val nsHandler = new DefaultUnknownNSHandler(logicalBuckets, 0)

    def setDefaultPartition(i:Int):Unit = {
        if ( i < 0 || i >= logicalBuckets.size ) {
            throw new IllegalArgumentException("invalid default partition index: " + i)
        }
        nsHandler.defaultPartition = i
    }

    def getEnv():Environment = {
        implicit val env = new Environment
        env.placement = dataPlacementMachine.getLocalNode(nsHandler)
        env.session = new TrivialSession
        env.executor = new TrivialExecutor
        env
    }

    private def buildLogicalBuckets():List[List[StorageMachine]] = {
        var logicalBuckets = List[List[StorageMachine]]()

        // We are enforcing the constraint that the number of dataplacement
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

    private def allMachines(): List[RClusterNode] = {
        storageMachines.map( (m) => m.machine ) ::: List(dataPlacementMachine.machine)
    }

    private def stopAllServices(): Unit = {
        allMachines.foreach(_.services.foreach(_.stop))
        allMachines.foreach(_.cleanServices)
    }

    def shutdown():Unit = {
        stopAllServices
    }

    def deploy():Unit = {
        deploy(true)
    }

    def deploy(stopServices:Boolean):Unit = {

        // Set up the remote logger
        val remoteLogger = Logger.getLogger("deploylib.remoteMachine")
        remoteLogger.setLevel(debugLevel)

        // Iterate over all the storage + dataplacement nodes to
        // clean up any pre-existing services, so we start fresh
        if (stopServices) stopAllServices

        // Give the services a chance to clean
        // TODO: we really need to have blocking command execution, this
        // is not the right way to do this
        Thread.sleep(1000)

        // Check to see if the port is open on the remote machine
        // for each node
        (storageMachines.map[ThriftMachine]( (m) => m ) ++ List(dataPlacementMachine)).foreach((machine) => {
            if ( !machine.getMachine.isPortAvailableToListen(machine.getThriftPort) ) {
                val msg = "Port " + machine.getThriftPort + " is in use on " + machine.getMachine
                logger.fatal(msg)
                throw new RemotePortInUseException(msg)
            }
        })

        // Start the storage engine service on all the storage nodes
        storageMachines.foreach( (rnode) => {
            val port = rnode.thriftPort
            // Setup runit on the node
            rnode.machine.setupRunit
            val storageNodeService = new JavaService(
                "../../../scalaengine/target/scalaengine-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.storage.JavaEngine","-p " +port, "-Xms512M -Xmx2048M")
            storageNodeService.action(rnode.machine)
            rnode.machine.services(0).watchLog
            rnode.machine.services(0).start
            ScadsDeployUtil.blockUntilRunning(rnode.machine.services(0))
        })

        // Start up the data placement node
        val rnode = dataPlacementMachine.machine
        val port = dataPlacementMachine.thriftPort
        val dataPlacementNodeService = new JavaService(
            "../../../placement/target/placement-1.0-SNAPSHOT-jar-with-dependencies.jar","edu.berkeley.cs.scads.placement.SimpleDataPlacementApp",port.toString)
        dataPlacementNodeService.action(rnode)
        //rnode.services(0).watchLog
        rnode.services(0).start
        ScadsDeployUtil.blockUntilRunning(rnode.services(0))

    }



    def rebalance():Unit = {
        val knownNs = dataPlacementMachine.getKnownNS
        var bucketMap = Map[String,List[Int]]()
        knownNs.foreach( (ns) => {

            var bucketList = List[Int]()

            logicalBuckets.foreach( (partition) => {

                var bucketCount = 0
                var seenOne = false

                partition.foreach( (smachine) => {

                    logger.debug("Looking at node: " + smachine.machine)
                    val dp = dataPlacementMachine.lookup_node(ns, smachine)
                    logger.debug("Returns DP: " + dp)
                    if ( isValidDataPlacement(dp) ) {
                        logger.debug("DP is valid...")
                        val count = smachine.totalCount(ns)
                        //dp.rset.range.setLimit(1)
                        //val startR = rhandle.get_set(ns, dp.rset)
                        //dp.rset.range.setOffset(count-1)
                        //val endR = rhandle.get_set(ns, dp.rset)

                        if ( !seenOne ) {
                            bucketCount = count
                        } else if ( bucketCount != count ) {
                           throw new InconsistentReplicationException("Found node " + smachine + " that has inconsistent data with the partition: bucketCount: " + bucketCount + " count: " + count)
                        }

                        seenOne = true

                        logger.debug("Storage node: " + smachine)
                        logger.debug("Count: " + count)
                        //logger.debug("Starting key: " + startR)
                        //logger.debug("Ending Key: " + endR)
                    } else {
                        logger.debug("No Data Placement for " + smachine)
                        if ( bucketCount != 0 ) {
                           throw new InconsistentReplicationException("Found node " + smachine + " that has no data, but the partition does")
                        }
                    }

                })

                bucketList = bucketList ::: List(bucketCount)

            })

            bucketMap += ( ns -> bucketList )


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

                        val dp = dataPlacementMachine.lookup_node(ns,smachine1)
                        logger.debug("current node has dp: " + dp)
                        assert( isValidDataPlacement(dp) )

                        val plus1 = smachine1.getNthRecord(ns,keylistPP)
                        logger.debug("KEYLISTPP+1-th elem :" + plus1)
                        val plus1keyval = plus1.key
                        logger.debug("KEYLISTPP+1-th key :" + plus1keyval)

                        val rset = ScadsDeployUtil.getRS(dp.rset)
                        rset.range.setOffset(keylistPP)
                        logger.debug("calling move()")
                        dataPlacementMachine.move(
                            ns, rset, smachine1, smachine2)
                        //val toRemove = handle1.get_set(ns,rset)
                        //logger.debug("TO REMOVE FROM " + tuple1._1 + ": " + toRemove)

                        //val listDp = new java.util.LinkedList[DataPlacement]()
                        //listDp.add(dp)
                        //dpHandle.remove(ns, listDp)
                        //dp.rset.range.setEnd_key((StringField(plus1keyval)).serialize)
                        dp.rset.range.setEnd_key(plus1keyval) // update current node

                        logger.debug("adding " + dp)
                        dataPlacementMachine.useConnection((c)=>c.add(ns, ListConversions.scala2JavaList(List(dp))))

                        val nextDp = dataPlacementMachine.lookup_node(ns,smachine2)
                        if ( isValidDataPlacement(nextDp) ) {
                            logger.debug("found valid existing dp:" + nextDp)
                            nextDp.rset.range.setStart_key(plus1keyval)
                            logger.debug("modified existing dp to be: " + nextDp)
                            dataPlacementMachine.useConnection((c)=>c.add(ns, ListConversions.scala2JavaList(List(nextDp))))
                        } else {

                            logger.debug("found no valid existing dp")

                            val endrset = ScadsDeployUtil.getRS(plus1keyval,null)
                            val newDp = smachine2.getDataPlacement(endrset)
                            dataPlacementMachine.useConnection((c)=>c.add(ns, ListConversions.scala2JavaList(List(newDp))))

                            //val endrset = new RecordSet()
                            //endrset.setType(3)
                            //val endrange = new RangeSet()
                            //endrset.setRange(endrange)
                            //endrange.setStart_key((StringField(plus1keyval)).serialize)
                            //endrange.setStart_key(plus1keyval)
                            //listDp.clear
                            //val newDp = new DataPlacement( tuple2._1.hostname, tuple2._2, tuple2._2, endrset )
                            //listDp.add(newDp)
                            //logger.debug("adding new dp: " + newDp)
                            //dpHandle.add(ns, listDp)

                        }


                    } else if ( curSize < keylistPP ) {
                        logger.debug("curSize<keylistPP case")
                        logger.debug("need to move in " + diff + " keys!")
                        val myDp = dataPlacementMachine.lookup_node(ns,smachine1)
                        logger.debug("MY DP: " + myDp)
                        //val myDp2 = dpHandle.lookup_node( ns, tuple1._1.hostname, tuple1._2, tuple1._2 )
                        //logger.debug("MY DP2: " + myDp2)
                        var keysMoved = 0
                        var sourceOffset = 1
                        var sourceMachine:StorageMachine = smachine2
                        var curDp:DataPlacement = null
                        while ( keysMoved < diff ) {
                            // need to move guys in!

                            curDp = dataPlacementMachine.lookup_node(ns,smachine2)

                            val nKeysBefore = sourceMachine.totalCount(ns)
                            if ( nKeysBefore != 0 ) {
                                logger.debug("Found keys in source node: " + sourceMachine.machine)
                                val rset = ScadsDeployUtil.getRS(curDp.rset)
                                rset.range.setLimit(diff-keysMoved)
                                dataPlacementMachine.move(
                                    ns, rset, sourceMachine, smachine1)
                                val nKeysAfter = sourceMachine.totalCount(ns)
                                keysMoved += (nKeysBefore - nKeysAfter)
                                logger.debug("Moved " + (nKeysBefore-nKeysAfter) + " from " + sourceMachine.machine + " to " + smachine1.machine)

                                if ( nKeysAfter == 0 ) {
                                    logger.debug(sourceMachine.machine + " is now empty!")
                                    // source is empty
                                    val dpToRemove = curDp
                                    dataPlacementMachine.useConnection((c)=>c.remove(ns, ListConversions.scala2JavaList(List(curDp))))
                                } else {
                                    // source still has keys- need to update the
                                    // start key of the guy
                                    logger.debug(sourceMachine.machine + " still has keys")
                                    //range.setLimit(1)
                                    //val firstKeyList = sourceNode.getHandle().get_set(ns, rset)
                                    //logger.debug("firstKeyList: " + firstKeyList)
                                    //assert( firstKeyList.length == 1, "must have a first key" )
                                    val firstKey = sourceMachine.getNthRecord(ns, 0)
                                    val dpToModify = curDp
                                    dpToModify.rset.range.setStart_key(firstKey.key)
                                    logger.debug("DP TO MODIFY IS: " + dpToModify)
                                    dataPlacementMachine.useConnection((c) => c.add(ns, ListConversions.scala2JavaList(List(dpToModify))))
                                    assert( keysMoved == diff, "didnt move all possible from node" )
                                    val moddedDP = dataPlacementMachine.lookup_node(ns, sourceMachine)
                                    logger.debug("Modded DP: " + moddedDP)
                                }
                            }

                            if ( keysMoved < diff ) {
                                sourceOffset += 1
                                sourceMachine = logicalBuckets(i+sourceOffset)(j)
                            }

                            //val toMovein = handle2.get_set(ns,rset)
                            //logger.debug("TO MOVE IN FROM " + tuple2._1 + ": " + toMovein)
                        }


                        var hasKey = sourceMachine.totalCount(ns) > 0
                        while ( !hasKey && (i+sourceOffset) < logicalBuckets.size-1 ) {
                            sourceOffset += 1
                            sourceMachine = logicalBuckets(i+sourceOffset)(j)
                            hasKey = sourceMachine.totalCount(ns) > 0
                        }

                        var thisDp = myDp
                        logger.debug("THIS DP: " + thisDp)
                        if ( !isValidDataPlacement(thisDp) ) {
                            thisDp = smachine1.getDataPlacement(ScadsDeployUtil.getAllRangeRecordSet)
                        }

                        if ( hasKey ) {
                            val nPlus1Record = sourceMachine.getNthRecord(ns,0)
                            thisDp.rset.range.setEnd_key(nPlus1Record.key)
                        } else {
                            thisDp.rset.range.setEnd_key(null)
                        }

                        logger.debug("MODIFYING thisDp to be: " + thisDp)

                        dataPlacementMachine.useConnection((c)=>c.add(ns,ListConversions.scala2JavaList(List(thisDp))))

                    }
                }

            }


            logger.debug("NAMESPACE : " + ns)
            logger.debug("-------------------BEGIN AFTER DATA DUMP ------------------")
            dumpNodeData(ns)
            logger.debug("-------------------END AFTER DATA DUMP ------------------")


        })

        dataPlacementMachine.getLocalNode.refreshPlacement

    }

    private def dumpNodeData():Unit = {
        val knownNs = dataPlacementMachine.getKnownNS
        knownNs.foreach( (ns) => dumpNodeData(ns) )
    }

    private def dumpNodeData(ns: String):Unit = {
        logicalBuckets.foreach( (partition) => {
            partition.foreach( (smachine) => {
                logger.debug("Looking at node: " + smachine.machine)
                val dp = dataPlacementMachine.lookup_node(ns, smachine)
                logger.debug("Returns DP: " + dp)
                logger.debug("Data:" + smachine.totalSet(ns))
            })
        })
    }



}
