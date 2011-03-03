package edu.berkeley.cs
package scads
package piql
package modeling

import avro.marker._

case class R1(var f1: Int) extends AvroPair {
  var v = 1 //HACK: Required due to storage engine bug
}

case class R2(var f1: Int, var f2: Int) extends AvroPair {
  var v = 1 //HACK: Required due to storage engine bug
}
