package optional.examples;

object Hello extends Application
{
  def main(
    times: Option[Int],
    greeting: Option[String],
    name: Option[String],
    arg1: String)
  {
    println("   Raw arguments: " + getRawArgs())
    println("Unprocessed args: " + getArgs())
    
    val strs = List.make(times getOrElse 1, greeting.getOrElse("hello") + " " + name.getOrElse("world"))
    println(strs mkString " / ")
  }
}
