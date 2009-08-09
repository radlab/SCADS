package optional.examples

import java.io.File

class Whatever(x: String) {
  override def toString() = "Whatever(" + x + ")"
}

object Hello extends optional.Application
{
  def main(
    times: Option[Int],
    greeting: Option[String],
    name: Option[String],
    file: Option[File],
    what: Option[Whatever],
    arg1: String,
    arg2: Double)
  {
    println("   Raw arguments: " + getRawArgs())
    println("Unprocessed args: " + getArgs())
    for (f <- file) println("File path: " + f.getAbsolutePath())
    for (w <- what) println("Whatever: " + w)
    
    val strs = List.make(times getOrElse 1, greeting.getOrElse("hello") + " " + name.getOrElse("world"))
    println(strs mkString " / ")
  }
}
