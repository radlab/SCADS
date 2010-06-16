package edu.berkeley.cs.scads.twitavro

import twitter._
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import java.io.File

import org.json._

import edu.berkeley.cs.scads.test.LongRec

class TwitterAvroFileIter[RecType <: SpecificRecord](f: File)(implicit manifest: scala.reflect.Manifest[RecType]) extends Iterator[RecType] {
  val reader = new DataFileReader(f, new SpecificDatumReader(manifest.erasure)) 
  def hasNext = reader.hasNext
  def next = reader.next.asInstanceOf[RecType]
}

class TwitterJSONFileIter(f: File) extends Iterator[Tweet] {
  val ignoreList = List[String]("type")

  var curRec:Tweet = null
  var tnum = 0
  var done = false
  val breader = 
    try {
      new java.io.BufferedReader(new java.io.FileReader(f))
    } catch {
      case e:Throwable => {
        println("Couldn't open file")
        e.printStackTrace()
        done = true
        null
      }
    }

  private def parseObject(obj:JSONObject, target:SpecificRecord) {
    val kit = obj.keys
    val schema = target.getSchema
    while (kit.hasNext) {
      val k = kit.next.toString
      if (!ignoreList.exists(_ ==k)) {
        val v = obj.get(k)
        v match {
          case s:String => {
            val f = schema.getField(k)
            if (f == null) {
              println("Warning, couldn't find field for String in target object: "+k)
            } else {
              target.put(f.pos,new Utf8(s))
            }
          }
          case n:Number => {
            val f = schema.getField(k)
            if (f == null) {
              println("Warning, couldn't find field for Number in target object: "+k)
            } else {
              target.put(f.pos,new java.lang.Long(n.longValue))
            }
          }
          case b:java.lang.Boolean => {
            val f = schema.getField(k)
            if (f == null) {
              println("Warning, couldn't find field for Boolean in target object: "+k)
            } else {
              target.put(f.pos,b)
            }
          }
          case ja:JSONArray => {
            val f = schema.getField(k)
            if (f == null) {
              println("Warning, couldn't find field for JSONArray in target object: "+k)
            } else {
              if (ja.length > 0) {
                ja.get(0) match {
                  case n:Number => { // we'll only handle this for the moment
                    var arraySchema = f.schema
                    while (arraySchema.getType != Schema.Type.ARRAY) {
                      if (arraySchema.getType != Schema.Type.UNION)
                        throw new Exception("Don't know how to handle non-union types while looking for an array schema")
                      val ta = arraySchema.getTypes
                      for (i<-(0 to (ta.size-1))) {
                        if (ta.get(i).getType == Schema.Type.ARRAY)
                          arraySchema = ta.get(i)
                      }
                    }
                    val avArray = new GenericData.Array[Double](ja.length,arraySchema)
                    for (i<-(0 to (ja.length-1))) 
                      avArray.add(ja.getDouble(i))
                    target.put(f.pos,avArray)
                  }
                  case o => {
                    println("Warning: don't handle arrays of not numbers at the moment")
                  }
                }
              }
            }
          }
          case jo:JSONObject => {
            val f = schema.getField(k)
            if (k == null) {
              println("Warning, couldn't find field for JSONObject in target object: "+k)
            } else {
              val newobj = 
                if (k == "user")
                  new user
                else if (k == "geo")
                  new point
                else if (k == "retweeted_status")
                  new Tweet
                else
                  null
              if (newobj != null) 
                parseObject(jo,newobj)
              else
                println("Warning: got an object I don't know how to handle: "+k)
              target.put(f.pos,newobj)
            }
          }
          case o => {
            if (o != JSONObject.NULL) 
              println("WARNING: Got type I don't know how to handle for "+k+": "+o.getClass.getName)
          }
        }
      }
    }
  }

  private def getNextRec():Tweet = {
    try {
      val line = breader.readLine
      tnum += 1
      if (line == null) {
        done = true
        null
      }
      else {
        val jobj = new JSONObject(line)
        val tweet = new Tweet
        val user = new user
          val point = new point
        parseObject(jobj,tweet)
        if (tweet.id == 0)
          throw new Exception("Tweets must have an ID")
        tweet
      }
    } catch {
      case e:Throwable => {
        println("Error parsing line "+tnum+" from file:")
        e.printStackTrace()
        getNextRec
      }
    }
  }

  def hasNext():Boolean = {
    if (curRec == null)
      curRec = getNextRec
    !done
  }

  def next():Tweet = {
    if (curRec == null)
      getNextRec
    if (done) 
      null
    else {
      val tmp = curRec
      curRec = getNextRec
      tmp
    }
  }

}

class TwitterIterWithKeys(f:File) extends Iterable[(LongRec,Tweet)] {
  private class WrapIt(f:File) extends Iterator[(LongRec,Tweet)] {
    val ti = new TwitterJSONFileIter(f)
    def hasNext:Boolean = ti.hasNext
    def next():(LongRec,Tweet) = {
      val t = ti.next
      val lr = new LongRec
      lr.f1 = t.id.longValue
      (lr,t)
    }
  }
  override def iterator():Iterator[(LongRec,Tweet)] = {
    new WrapIt(f)
  }
}
