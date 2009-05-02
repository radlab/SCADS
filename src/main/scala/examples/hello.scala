package optional.examples;

object Hello extends Application{
  def main(times : Option[Int], greeting : Option[String], name : Option[String], args : Array[String]){
    for(i <- 0 until times.getOrElse(1))
      println(greeting.getOrElse("hello") + " " + name.getOrElse("world"));
    println("Arguments:");
    args.foreach(x => println("  " + x));
  }
}
