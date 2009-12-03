
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.log4j.Level._

import edu.berkeley.cs.scads.deployment._

import edu.berkeley.cs.scads.thrift.{RangedPolicy,StorageNode}

import org.apache.zookeeper.{CreateMode,ZooKeeper}
import org.apache.zookeeper.ZooDefs.Ids

import scala.collection.jcl.Conversions

val zooKeeper = new ZooKeeper("localhost:2181",3000,null)
recursiveDelete("/scads/namespaces")
zooKeeper.create("/scads/namespaces", "".getBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)

val clusterDeploy = new ClusterDeploy("localhost",2181,2)
clusterDeploy.getPartition(3).foreach(Queries.configureStorageEngine(_)) // this hack lets us know of all the namespaces we'll be dealing with, so we can explicitly zero out the policies in the storage nodes
val knownNS = clusterDeploy.getAllNamespaces

val storageURIs = (9000 to 9013)
val storageNodes = storageURIs.map(new StorageNode("localhost",_))
storageNodes.foreach( (n) => {
    knownNS.foreach( (ns) => { 
        n.useConnection((c) => { c.remove_set(ns, RangedPolicy.convert((null,null)).get(0)) })
        n.useConnection((c) => {
            c.set_responsibility_policy(ns,ListConversions.scala2JavaList(List()))
        })
    })
})

clusterDeploy.getPartition(3).foreach(Queries.configureStorageEngine(_))

implicit val env = clusterDeploy.getEnv

println("-------------- LOADING USERS 0 -> 51 -----------------")
for(i <- 0 until 51) {
    val user = new user
    user.name("user"+i)
    user.save
}

println("------------- CALLING REBALANCE ----------------")
clusterDeploy.rebalance

println("------------- ATTEMPTING TO RETRIEVE 0 -> 51 ----------")
for(i <- 0 until 51) { 
    println(Queries.finduserByPK("user"+i))
}
println("------------- SUCCESSFULLY RETRIEVED 0 -> 51 ----------")


println("-------------- LOADING USERS 51 -> 83 -----------------")
for(i <- 51 until 83) {
    val user = new user
    user.name("user"+i)
    user.save
}

println("------------- ATTEMPTING TO RETRIEVE 51 -> 83 BEFORE REBALANCE----------")
for(i <- 51 until 83) { 
    println(Queries.finduserByPK("user"+i))
}
println("------------- SUCCESSFULLY RETRIEVED 51 -> 83 BEFORE REBALANCE ----------")

println("------------- CALLING REBALANCE ----------------")
clusterDeploy.rebalance

println("------------- ATTEMPTING TO RETRIEVE 0 -> 83 AFTER REBALANCE----------")
for(i <- 0 until 83) { 
    println(Queries.finduserByPK("user"+i))
}
println("------------- SUCCESSFULLY RETRIEVED 0 -> 83 AFTER REBALANCE ----------")


def recursiveDelete(znode:String):Unit = {
    if ( zooKeeper.exists(znode,false) == null ) return
    val children = zooKeeper.getChildren(znode,null)
    if ( children.size > 0 ) {
        Conversions.convertList(children).toList.map(znode+"/"+_).foreach(recursiveDelete(_))
    }
    zooKeeper.delete(znode,-1)
}
