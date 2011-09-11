package org.chris.features

import scala.collection.mutable._
import scala.io.Source._

import java.io._
import java.text._

import org.codehaus.jackson.map.{DeserializationConfig,ObjectMapper,MappingJsonFactory}
import org.codehaus.jackson._


object Compare {

    //def init():
    //    ParseFiles.modes["skip_kestrel"] = true
    //    ParseFiles.modes["skip_email"] = true
    //    ParseFiles.modes["skip_tweet"] = true
    //    ParseFiles.modes["skip_html"] = false
    //    ParseFiles.modes["skip_headers"] = false
    //    ParseFiles.modes["skip_individual_ips"] = true
    //    ParseFiles.modes["use_ngram"] = false

    def generateMap(fileName : String) : HashMap[String,Int] = {
        var mapper = new ObjectMapper()
        mapper.getDeserializationConfig().set(DeserializationConfig.Feature.USE_BIG_INTEGER_FOR_INTS, true)
        val data = fromFile(fileName).getLines.mkString

        try {
            implicit val obj = mapper.readValue(data, classOf[java.util.HashMap[String,Object]])
            val newrec = ParseFiles.createRecord(obj)
            val vector = newrec.vectorize("record")
            val binaryVector = new HashMap[String,Int]
            vector.foreach(kv => binaryVector += kv._1 -> toBinary(kv._2))
            return binaryVector 
        }
        catch {
            case _ => 
                return new HashMap[String,Int]
        }
    }

    def toBinary(i : Any) : Int = {
        try {
            if (i.asInstanceOf[Int] > 0) {
                return 1
            }
            else {
                return 0
            }
        }
        catch {
            case e : java.lang.ClassCastException =>
                return 0
        }
    }

    def mergeMap[A, B](ms: List[Map[A, B]])(f: (B, B) => B): Map[A, B] =
          (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
          a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)}

    def main(args : Array[String]) {
        //val dir = new File("/data/twitter/crawler/2010-09-18/23/")
        //val fileList = dir.listFiles

        val fileList = fromFile("spamfiles.txt").mkString.split('\n').toList
        val results = fileList.map(f => generateMap(f)).toList
        val finalResult = mergeMap(results)((v1, v2) => v1 + v2)

        println(finalResult)
    }   
}
