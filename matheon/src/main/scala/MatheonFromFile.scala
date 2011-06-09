package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ArrayBuilder

import java.io.{BufferedReader,FileReader}


object MatheonFromFile {
  
  def main(args:Array[String]) {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[IntRec, MReading]("matheonNs")

    println("Parsing data from: "+args(0))

    var k = 0
    val br = new BufferedReader(new FileReader(args(0)))
    var l = br.readLine
    
    var puts = new ArrayBuilder.ofRef[(IntRec,MReading)]()

    while (l != null) {
      val sp = l.indexOf(' ')
      val mass = java.lang.Double.parseDouble(l.substring(0,sp))
      val cnt = Integer.parseInt(l.substring(sp+1))
      puts += ((IntRec(k), MReading(mass,cnt)))
      l = br.readLine
      k+=1
    }

    
    println("Loading data...")
    ns ++= puts.result
    println("data loaded")


    val peaks = ns.applyAggregate(List[String](),
                                  classOf[IntRec].getName,
                                  classOf[MReading].getName,
                                  List(),
                                  List((new PeaksLocal,new PeaksRemote(3.0,500))))
    println(peaks)
  }
}
