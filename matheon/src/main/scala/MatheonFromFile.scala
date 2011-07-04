package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ArrayBuilder

import java.io.{BufferedReader,FileReader,InputStream,InputStreamReader}

class MatheonParser extends RecParser {
  private var fileId = 0
  private var k = 0
  private var reader:BufferedReader = null

  override
  def setLocation(location:String) {
    val usidx = location.indexOf('_')
    fileId = Integer.parseInt(location.substring(usidx+1,location.indexOf('.',usidx)))
    println("Location: "+location+" (fileId: "+fileId+")")
    k = 0
  }

  def setInput(in:InputStream):Unit = {
    if (reader != null)
      reader.close()
    reader = new BufferedReader(new InputStreamReader(in))
  }

  def getNext():(Array[Byte],Array[Byte]) = {
    val line = reader.readLine()
    if (line == null)
      return null
    val sp = line.indexOf(' ')
    val mass = java.lang.Double.parseDouble(line.substring(0,sp))
    val cnt = java.lang.Float.parseFloat(line.substring(sp+1))
    val mrb = MReading(fileId,mass,cnt).toBytes

    val buffer = java.nio.ByteBuffer.allocate(mrb.length + 16)
    buffer.putLong(System.currentTimeMillis)
    buffer.putLong(0)
    buffer.put(mrb)
    
    val ret = (MatheonKey(fileId,k).toBytes, buffer.array)
    k+=1
    ret
  }
}


object MatheonFromFile {
  
  def main(args:Array[String]) {
    val cluster = TestScalaEngine.newScadsCluster()
    val ns = cluster.getNamespace[MatheonKey, MReading]("matheonNs")

    println("Starting bulk load")
    ns.putBulkLocations(new MatheonParser, args.map("file://"+_),
                        None,None)
    println("Done")


    for (i <- 1 to 4) {
      val st = System.nanoTime
      val peaks = ns.applyAggregate(List[String]("fileId"),
                                  classOf[MatheonKey].getName,
                                  classOf[MReading].getName,
                                  List(),
                                  List((new PeaksLocal,new PeaksRemote(3.0,500))))
      val et = System.nanoTime
      val t = et-st
      println("Time: "+(t)+ " ("+(t/1000000)+" milliseconds)")
      println(peaks)
    }
  }
}
