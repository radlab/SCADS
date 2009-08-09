package optional.examples

import java.io.File
import java.lang.reflect

class Whatever(x: String) {
  override def toString() = "Whatever(" + x + ")"
}

object Hello extends optional.Application
{
  // testing excluding conversions
  override def isConversionMethod(m: reflect.Method) = m.getName != "dingle"
  
  // implicit def stringToSet(s: String): Set[Char] = Set(s : _*)
  def optionStringSet(s: String): Set[Char] = Set(s : _*)
  def optionFooBar(s: String): List[Float] = List(1.1f, 2.2f)
  def dingle(s: String): List[Int] = List(1,2,3)
  
  def main(
    times: Option[Int],
    greeting: Option[String],
    name: Option[String],
    file: Option[File],
    what: Option[Whatever],
    // ints: Option[List[Int]],
    xs: Option[Set[Char]])
    // arg1: String,
    // arg2: Double)
  {
    println("   Raw arguments: " + getRawArgs())
    println("Unprocessed args: " + getArgs())
    for (f <- file) println("File path: " + f.getAbsolutePath())
    for (w <- what) println("Whatever: " + w)
    if (xs.isDefined) {
      for (x <- xs.get) println("... " + x)
    }
    
    val strs = List.make(times getOrElse 1, greeting.getOrElse("hello") + " " + name.getOrElse("world"))
    println(strs mkString " / ")
  }
}
