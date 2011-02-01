package edu.berkeley.cs
package twitterspam

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import org.chris.features.ParseFiles
import org.chris.features.ParseFiles._
import org.codehaus.jackson.JsonParseException
import org.codehaus.jackson.map.{DeserializationConfig, ObjectMapper, MappingJsonFactory}
import java.security.MessageDigest

object ParseData {
  def parseData(str: String, exclusions: Array[String] = Array()): SparseVectorOriginal = {
    var mapper = new ObjectMapper
    mapper.getDeserializationConfig().set(DeserializationConfig.Feature.USE_BIG_INTEGER_FOR_INTS, true)

    // reset the mode
    //for ((k, v) <- ParseFiles.modes) {
    //  ParseFiles.modes(k) = false
    //}
    //for ((k, v) <- ParseFiles.only_modes) {
    //  ParseFiles.only_modes(k) = false
    //}
    //ParseFiles.ngram_n = 0

    try {
      loadArguments(exclusions)
      implicit val obj = mapper.readValue(str, classOf[java.util.HashMap[String,Object]])
      val newrec = createRecord(obj)
      val vector = newrec.vectorize("record")
      val finalVector = SparseVectorOriginal.zeros

      for ((k, v) <- vector) {
        var processable = true
        val newK = k.take(256)
        //for (ex <- exclusions) {
        //  if (k.matches(ex)) {
        //    processable = false
        //  }
        //}
        if (processable) {
          v match {
            case x: Int => finalVector(newK) = x.toDouble
            case x: Double => finalVector(newK) = x
            case _ => 
          }
        }
      }
      finalVector
    }
    catch {
      case e: JsonParseException =>
        println("Failed to parse %s".format(str))
        SparseVectorOriginal.zeros

      case _ =>
        println("Generic parse failure %s".format(str))
        SparseVectorOriginal.zeros
    }
  }

  val hashLength = 4

  def parseDataHashed(str: String, exclusions: Array[String] = Array()): SparseVector = {
    var mapper = new ObjectMapper
    mapper.getDeserializationConfig().set(DeserializationConfig.Feature.USE_BIG_INTEGER_FOR_INTS, true)
    val md = MessageDigest.getInstance("MD5")

    // reset the mode
    for ((k, v) <- ParseFiles.modes) {
      ParseFiles.modes(k) = false
    }
    ParseFiles.ngram_n = 0

    try {
      loadArguments(exclusions)
      implicit val obj = mapper.readValue(str, classOf[java.util.HashMap[String,Object]])
      val newrec = createRecord(obj)
      val vector = newrec.vectorize("record")
      val finalVector = SparseVector.zeros

      for ((k, v) <- vector) {
        var processable = true
        if (processable) {
          v match {
            case x: Int => finalVector(k) = x.toDouble
            case x: Double => finalVector(k) = x
            case _ => 
          }
        }
      }
      finalVector
    }
    catch {
      case e: JsonParseException =>
        println("Failed to parse %s".format(str))
        SparseVector.zeros

      case _ =>
        println("Generic parse failure %s".format(str))
        SparseVector.zeros
    }
  }
}
