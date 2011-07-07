package edu.berkeley.cs.scads.matheon

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.comm._

import scala.collection.mutable.ArrayBuilder

import java.io.{BufferedReader,FileReader,InputStream,InputStreamReader}


object MatheonSizeComputer {

  private val s_runtime = Runtime.getRuntime();

  private def runGC() = {
    // for whatever reason it helps to call Runtime.gc()
    // using several method calls:
    var i = 0
    while (i < 4) {
      _runGC()
      i+=1
    }
  }

  private def _runGC() = {
    var usedMem1:Long = usedMemory()
    var usedMem2 = scala.Long.MaxValue
    var i = 0

    while((usedMem1 < usedMem2) && (i < 1000)) {
      s_runtime.runFinalization();
      s_runtime.gc();
      //Thread.currentThread().`yield()`;
      Thread.`yield`()

      usedMem2 = usedMem1;
      usedMem1 = usedMemory();
      i+=1
    }
  }

  private def usedMemory():Long = {
    s_runtime.totalMemory() - s_runtime.freeMemory();
  }


  def main(args:Array[String]) {
    runGC();
    usedMemory();


    val count = 1000000 // 10000 or so is enough for small ojects
    val objects:Array[Object] = new Array[Object](count)

    var heap1:Long = 0;

    var i = -1

    while(i < count) {
      var obj:Object = null
      //// INSTANTIATE YOUR DATA HERE AND ASSIGN IT TO 'object':
      
      //obj = MatheonKey(3430)
      obj = MReading(2144,3432432.343f,3243.34f)
      //obj = new java.lang.Integer(3430)
      
      
      if (i >= 0) {
        objects(i) = obj;
      }
      else {
	obj = null; // discard the "warmup" object
	runGC()
	heap1 = usedMemory() // take a "before" heap snapshot
      }
      i+=1
    }
  
    runGC()
    val heap2 = usedMemory()

    val size = scala.math.round(((heap2 - heap1).asInstanceOf[Float] / count))
    println("'before' heap: " + heap1 + ", 'after' heap: " + heap2)
    println("heap delta: " + (heap2 - heap1) + ", {" + objects(0).getClass() + "} size = " + size + " bytes")
  }
}
