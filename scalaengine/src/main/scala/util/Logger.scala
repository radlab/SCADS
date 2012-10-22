package edu.berkeley.cs.scads.util

/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: 11/2/11
 * Time: 11:01 AM
 * To change this template use File | Settings | File Templates.
 */

object Logger {

  def apply[T](cls : Class[T]) : net.lag.logging.Logger = {
    val l = net.lag.logging.Logger(cls.toString.replaceAll("class ", "") + ".t")
//    l.setLevel(java.util.logging.Level.FINEST)
    l
  }

}
