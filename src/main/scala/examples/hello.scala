package optional.examples;

object Hello extends Application
{
  def main(times: Option[Int], greeting: Option[String], name: Option[String]) {
    if (getArgs().size == 0)
      return println("Usage: Hello <...>")

    println("Raw argument list = " + getRawArgs())
    println("Unprocessed args = " + getArgs())
    
    for (i <- 0 until times.getOrElse(1))
      println(greeting.getOrElse("hello") + " " + name.getOrElse("world"));
      
  }
}
