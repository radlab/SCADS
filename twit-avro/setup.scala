import edu.berkeley.cs.scads.twitavro._
import twitter._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8

import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

settings.maxPrintString = 100000

TestScalaEngine
val k1 = new LongRec
k1.f1 = 1
val k2 = new Tweet

TestScalaEngine.cluster.createNamespaceIfNotExists("tweets", k1.getSchema(), k2.getSchema())

val ns = new Namespace[LongRec,Tweet]("tweets",10000,TestScalaEngine.zooKeeper)

val cr = new ConfigureRequest
cr.namespace = "tweets"
cr.partition = "1"
Sync.makeRequest(TestScalaEngine.node, new Utf8("Storage"), cr)

def loadData(file:String):Long = {
  val tf = new java.io.File(file)
  val tit = new TwitterIterWithKeys(tf)
  ns ++= tit
}

def findMinTweet():Long = {
  ns.foldLeft((new LongRec,new Tweet))((a:(LongRec,Tweet), kv:(LongRec,Tweet))=>{if((a._1.f1 == 0) || (a._1.f1 > kv._1.f1)) a._1.f1 = kv._1.f1;a})._1.f1
}

def findMaxTweet():Long = {
  ns.foldLeft((new LongRec,new Tweet))((a:(LongRec,Tweet), kv:(LongRec,Tweet))=>{if((a._1.f1 == 0) || (a._1.f1 < kv._1.f1)) a._1.f1 = kv._1.f1;a})._1.f1
}

def countGeoTweets():Int = {
  ns.filter((k,v)=>{v.geo != null}).toSeq.size
}
