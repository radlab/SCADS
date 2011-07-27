package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ArrayBuilder

import java.io.{BufferedReader,FileReader,InputStream,InputStreamReader}

class MatheonParser(inMem:Boolean = false) extends RecParser {
  private var fileId = 0
  private var k = scala.Int.MinValue
  private var reader:BufferedReader = null
  private var rcnt = 0
  private var fcnt = 0

  override
  def setLocation(location:String) {
    val usidx = location.indexOf('_')
    fileId = Integer.parseInt(location.substring(usidx+1,location.indexOf('.',usidx)))
    println("Location: "+location+" (fileId: "+fileId+") "+ "(files so far: "+fcnt+")")
    //println("Heapsize: "+(Runtime.getRuntime().totalMemory()/1000000))
    //println("Free: "+(Runtime.getRuntime().freeMemory()/1000000))
    //println("Estimated size so far: "+((50*rcnt)/1000000))
    //k = 0
    fcnt += 1
  }

  def setInput(in:InputStream):Unit = {
    if (reader != null)
      reader.close()
    reader = new BufferedReader(new InputStreamReader(in))
  }

  def getNext():(Array[Byte],AnyRef) = {
    val line = reader.readLine()
    if (line == null)
      return null
    val sp = line.indexOf(' ')
    val mass = java.lang.Float.parseFloat(line.substring(0,sp))
    val cnt = java.lang.Float.parseFloat(line.substring(sp+1))
    val mr = MReading(fileId,mass,cnt)

    if (inMem) {
      val ret = (MatheonKey(k).toBytes, mr)
      k+=1
      rcnt+=1
      ret
    } else {
      val mrb = mr.toBytes
      val buffer = java.nio.ByteBuffer.allocate(mrb.length + 16)
      buffer.putLong(System.currentTimeMillis)
      buffer.putLong(0)
      buffer.put(mrb)
      
      val ret = (MatheonKey(k).toBytes, buffer.array)
      k+=1
      ret
    }
  }
}

object MatheonFromFile {
  
  def main(args:Array[String]) {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[MatheonKey, MReading]("matheonNs")

    println("Starting bulk load of "+args.size+" files")
    var st = System.nanoTime
    ns.putBulkLocations(new MatheonParser(false), args.map("file://"+_),
                        None,None)
    var et = System.nanoTime
    println("Done")
    var t = et-st
    println("Load Time: "+(t)+ " ("+(t/1000000)+" milliseconds)\n[enter to continue]")
    System.in.read()
    
    st = System.nanoTime
    val peaks = ns.applyAggregate(List[String]("fileId"),
                                  classOf[MatheonKey].getName,
                                  classOf[MReading].getName,
                                  List(),
                                  List((new DetPeaksLocal,new DetPeaksRemote(1.0f))))
    et = System.nanoTime
    t = et-st
    println("Time: "+(t)+ " ("+(t/1000000)+" milliseconds)")
    println(peaks)

    /*
    for (i <- 1 to 4) {
      val st = System.nanoTime
      val peaks = ns.applyAggregate(List[String]("fileId"),
                                  classOf[MatheonKey].getName,
                                  classOf[MReading].getName,
                                  List(),
                                  List((new ZeroPeaksLocal,new ZeroPeaksRemote(3.0f,500))))
      val et = System.nanoTime
      val t = et-st
      println("Time: "+(t)+ " ("+(t/1000000)+" milliseconds)")
      println(peaks)
    }
    */
    
    /*
    for (i <- 1 to 1) {
      val st = System.nanoTime
      val peaks = ns.applyAggregate(List[String]("fileId"),
                                    classOf[MatheonKey].getName,
                                    classOf[MReading].getName,
                                    List(),
                                    List((new DetPeaksLocal,new DetPeaksRemote(1.0f))))
      val et = System.nanoTime
      val t = et-st
      println("Time: "+(t)+ " ("+(t/1000000)+" milliseconds)")
      val lsts = peaks(0)._2
      try {
        val mass = lsts(0).asInstanceOf[scala.Tuple2[_,_]]._1.asInstanceOf[Seq[Float]]
        val cnts = lsts(0).asInstanceOf[scala.Tuple2[_,_]]._2.asInstanceOf[Seq[Float]]
        val zipped = mass.zip(cnts)
        
        zipped.foreach(z => {
          println(z._1+","+z._2)
        })
        
      } catch{
        case e:Exception => e.printStackTrace
      }
    }
    */
  }
}
