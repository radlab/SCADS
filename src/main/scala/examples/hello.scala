package optional.examples;

object Hello extends Application{
  default("greeting", "hello");
  default("name", "world");

  def main(greeting : String, name : String, args : Array[String]){
    println(greeting + " " + name);
    println("Arguments:");
    args.foreach(x => println("  " + x));
  }
}
