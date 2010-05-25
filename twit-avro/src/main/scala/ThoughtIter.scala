package edu.berkeley.cs.scads.twitavro

import piql._
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.file.DataFileReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.util.Utf8
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import java.io.File

import org.json._

class ThoughtJSONFileIter(f: File) extends Iterator[(Thought,User)] {
  val ignoreList = List[String]("type")
	val df = new java.text.SimpleDateFormat("EEE MMM d HH:mm:ss Z yyyy")

  var curRec:(Thought,User) = null
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
  
  private def getNextRec():(Thought,User) = {
    try {
      val line = breader.readLine
      tnum += 1
      if (line == null) {
        done = true
        null
      }
      else {
        val idx = line.indexOf("{")
        val nl = 
          if (idx > 0)
            line.substring(idx)
          else
            line
        val jobj = new JSONObject(nl)
        val thought = new Thought
        val user = new User
        try {
          thought.value.text = jobj.get("text").toString
          val uobj = jobj.get("user").asInstanceOf[JSONObject]
          thought.key.owner.name = uobj.get("screen_name").toString
          val d = df.parse(jobj.get("created_at").toString)
          val lt = d.getTime
          thought.key.timestamp = (lt/1000).intValue
          user.key.name = uobj.get("screen_name").toString
          user.value.password = "pass"
          user.value.profileData = uobj.get("description").toString
          user.value.fullname = uobj.get("name").toString
          user.value.url = uobj.get("url").toString
        } catch {
          case e:Throwable => {
            println("Error parsing line: "+line)
            getNextRec
          }
        }
        (thought,user)
      }
    } catch {
      case e:Throwable => {
        println("Error parsing a line")
        getNextRec
      }
    }
  }

  def hasNext():Boolean = {
    if (curRec == null)
      curRec = getNextRec
    !done
  }

  def next():(Thought,User) = {
    if (curRec == null)
      curRec = getNextRec
    if (done) 
      null
    else {
      val tmp = curRec
      curRec = getNextRec
      tmp
    }
  }
}
