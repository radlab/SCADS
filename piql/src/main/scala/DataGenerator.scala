package edu.berkeley.cs.scads.piql


import scala.collection.mutable.HashSet


/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Oct 11, 2010
 * Time: 11:11:16 AM
 * To change this template use File | Settings | File Templates.
 */

object DataGenerator{

  /**
   * Generates count random numbers from [0, maxInt)
   */
  def randomInts(seed: Int, maxInt: Int, count: Int): Array[Int] = {
    require(maxInt >= 0)
    require(count >= 0)
    require(maxInt > count)

    val rand = new scala.util.Random(seed)
    val nums = new Array[Int](count)
    var pos = 0

    while(pos < count) {
      val randNum = rand.nextInt(maxInt)
      if(!(nums contains randNum)) { // TODO: use a set here
        nums(pos) = randNum
        pos += 1
      }
    }
    nums
  }
    /**
   * Generates count random strings
   */
  def randomStrings(seed: Int, count: Int): Array[String] = {
    require(count >= 0)

    val strlen = stringSize(count)

    val rand = new scala.util.Random(seed)

    val ret = new Array[String](count)
    val seenBefore = new HashSet[String]
    var pos = 0

    while (pos < count) {
      val chArray = new Array[Char](strlen)
      var i = 0
      while (i < strlen) {
        //chArray(i) = rand.nextInt(256).toChar
        chArray(i) = rand.nextPrintableChar() // easier to debug for now
        i += 1
      }
      val str = new String(chArray)
      if (!(seenBefore contains str)) {
        seenBefore += str
        ret(pos) = str
        pos += 1
      }
    }

    ret
  }

  def stringSize(num: Int): Int = {
    var size = 256
    var length = 1
    while (size < num) {
      size = size * 256
      length += 1
    }
    length
  }

}
