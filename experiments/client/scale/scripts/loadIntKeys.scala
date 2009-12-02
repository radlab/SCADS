import scaletest._
import deploylib.rcluster._
import edu.berkeley.cs.scads.thrift._

val nodes = List(r6,r8,r10,r11)
val testSize = 1000

nodes.foreach(_.cleanServices)

val serverConf = new ScadsEngine(9876, "r12:2181")
nodes.foreach(n => serverConf.action(n))

val storageServices = nodes.flatMap(_.services).filter(_.name == "edu.berkeley.cs.scads.storage.JavaEngine")
storageServices.foreach(_.watchLog)
storageServices.foreach(_.start)

Thread.sleep(10000)//FIXME

(0 to testSize by (testSize/nodes.size)).toList.zip(nodes).reduceLeft((s,e) => {
  val loaderConf = new IntKeyLoader(s._1,e._1)
  val node = new StorageNode(s._2.hostname, 9876)
  node.useConnection(_.set_responsibility_policy("intKeys", RangedPolicy.convert(("%010d".format(s._1),"%010d".format(e._1))))) 

  loaderConf.action(s._2)
  e
})

val loadingServices = nodes.flatMap(_.services).filter(_.name == "scaletest.LoadIntKeys")
loadingServices.foreach(_.watchLog) 
loadingServices.foreach(_.once)
