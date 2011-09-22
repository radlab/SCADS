package edu.berkeley.cs
package scads
package piql
package modeling

import storage._
import perf._
import comm._
import piql.plans._
import piql.opt._
import piql.scadr._
import perf.scadr._
import deploylib.ec2._
import avro.marker._
import avro.runtime._

import net.lag.logging.Logger
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

trait DataMaker {var data: String; def withDataSize(size: Int):this.type =
  {data = "*" * size; this}}
case class R1(var f1: Int) extends AvroPair with DataMaker { var data: String = _ }
case class R2(var f1: Int, var f2: Int) extends AvroPair with DataMaker { var data: String = _ }

case class RandomNumber(numTuples: Int) extends ParameterGenerator {
  final def getValue(rand: Random) = (rand.nextInt(numTuples), None)
}

case class RandomR1List(numTuples: Int, cardinalityList: IndexedSeq[Int]) extends ParameterGenerator {
  final def getValue(rand: Random) = {
    val cardinality = cardinalityList(rand.nextInt(cardinalityList.size))
    (ArrayBuffer.fill(cardinality)(ArrayBuffer(R1(rand.nextInt(numTuples)))), Some(cardinality))
  }
}

class GenericQueryProvider extends QueryProvider {
  val logger = Logger()

  def getQueryList(cluster: ScadsCluster, executor: QueryExecutor): IndexedSeq[QuerySpec] = {
    val clusterConfig = cluster.root.awaitChild("clusterReady")
    val loaderConfig = classOf[GenericRelationLoaderTask].newInstance.parse(clusterConfig.data)
    val r2 = cluster.getNamespace[R2]("R2")

    val randomNumber = RandomNumber(loaderConfig.numTuples)
    val inputSizes = RandomR1List(loaderConfig.numTuples, 50 to 500 by 50 toIndexedSeq)
    val limits = CardinalityList(50 to 500 by 50 toIndexedSeq)
    require(loaderConfig.maxCardinality >= limits.values.max, "max cardinality of cluster is too small for specified limits")

    def lim(ord: Int) = new ParameterLimit(ord, 100000)
    QuerySpec(
      new OptimizedQuery("indexScanQuery",
			 LocalStopAfter(lim(0),
			   IndexScan(r2, (1.?) :: Nil, lim(0), true)),
			 executor),
      Vector(limits, randomNumber)) ::
    QuerySpec(
      new OptimizedQuery("indexLookupJoin",
			 IndexLookupJoin(r2, 
					 AttributeValue(0,0) :: ConstantValue(0) :: Nil,
					 LocalIterator(0)),
			 executor),
      Vector(inputSizes)) ::
    QuerySpec(
      new OptimizedQuery("indexMergeJoin",
			 IndexMergeJoin(r2, 
					AttributeValue(0,0) :: ConstantValue(0) :: Nil,
					AttributeValue(1,1) :: Nil,
					lim(1),					
					true,
					LocalIterator(0)),
			 executor),
      Vector(inputSizes, limits)) :: Nil toIndexedSeq
  }
}

case class GenericRelationLoaderTask(var numServers: Int = 10,
				     var numLoaders: Int = 10,
				     var replicationFactor: Int = 2,
				     var tuplesPerServer: Int = 10000,
				     var dataSize: Int = 128,
				     var maxCardinality: Int = 500) extends DataLoadingTask with AvroRecord {
  var clusterAddress: String = _

  def numTuples = (numServers / replicationFactor) * tuplesPerServer

  def run(): Unit = {
    val clusterRoot = ZooKeeperNode(clusterAddress)
    val coordination = clusterRoot.getOrCreate("coordination/loaders")
    val cluster = new ExperimentalScadsCluster(clusterRoot)
    cluster.blockUntilReady(numServers)

    val clientId = coordination.registerAndAwait("clientStart", numLoaders)
    if(clientId == 0) {
      val keySplits = None +: (1 until (numServers/replicationFactor)).map(i => Some(R2(i * tuplesPerServer, 0)))
      val partitions = keySplits.zip(cluster.getAvailableServers.grouped(replicationFactor).toSeq)
      logger.info("Creating R2 with partitions: %s", partitions)
      cluster.createNamespace[R2]("R2", partitions)
    }
    
    coordination.registerAndAwait("startBulkLoad", numLoaders)
    val r2 = cluster.getNamespace[R2]("R2")
    val totalTuples = tuplesPerServer * (numServers / replicationFactor)
    val tuplesPerLoader = totalTuples / numLoaders
    val startKey = clientId * tuplesPerLoader
    val endKey = (clientId + 1) * tuplesPerLoader - 1

    logger.info("Loading %d keys from %d to %d out of %d total keys", endKey - startKey, startKey, endKey, totalTuples)
    r2 ++= (startKey to endKey).flatMap(i =>
      (1 to maxCardinality).map(j => R2(i,j).withDataSize(dataSize)))
    coordination.registerAndAwait("bulkLoadFinished", numLoaders)

    if(clientId == 0) {
      ExperimentNotification.completions.publish("generic cluster data load finished", this.toJson)
      clusterRoot.createChild("clusterReady", data=this.toBytes)
    }
  }
}
