package optional;

class DesignError(msg : String) extends Error(msg);
class InvalidCall(msg : String) extends Exception(msg);

/**
 *  This trait automagically finds a main method on the object 
 *  which mixes this in and based on method names and types figures
 *  out the options it should be called with and takes care of parameter parsing
 */ 
trait Application{

  private def designError(name : String) = throw new DesignError(name);
  private def invalidCall(name : String) = throw new InvalidCall(name);

  private lazy val mainMethod = {
    val candidates = 
      getClass.
      getMethods.
      filter(_.getName == "main").
      filter{x => 
        val args = x.getParameterTypes;
        // want to avoid the main method we define here
        (args.length != 1) || (args(0) != classOf[Array[String]]);
      }

    candidates.length match {
      case 0 => designError("No main method found");
      case 1 => candidates(0);
      case _ => designError("You seem to have multiple main methods. We found methods with signatures:\n" + candidates.map(_.getParameterTypes.mkString("  (", ", ", ")")).mkString("\n"));
    }
  }

  private lazy val argumentNames = 
    new com.thoughtworks.paranamer.BytecodeReadingParanamer().
    lookupParameterNames(mainMethod);      

  private lazy val argumentTypes = 
    mainMethod.getParameterTypes
  
  private val defaults = new scala.collection.mutable.OpenHashMap[String, String];

  /**
   * Set a default value for this option name. 
   * I hope this will eventually go away and be replaced by Just Doing The Right Thing
   * when you give an argument a default value. But for now it's needed, as that feature
   * isn't available yet.
   */
  def default(option : String, value : String) = 
    defaults(option) = value;
  // todo: deal with the case where it's just "arg" correctly
  private val Argument = new scala.util.matching.Regex("""arg(\d+)""") 
  private object Numeric{
    def unapply(x : String) = 
      try { Some(x.toInt) }
      catch { 
        case _ : java.lang.NumberFormatException 
          => None 
      }
  }

  def callWithOptions(options : Options) = {
    val methodArguments = new Array[AnyRef](argumentTypes.length);
    import options.{args => arguments};

    val opts = options.options.orElse(defaults); 

    for(i <- 0 until methodArguments.length){
      methodArguments(i) = argumentNames(i) match {
        case "args" | "arguments" => {
          val x = arguments.toArray[String];
          x // work around for retarded array boxing
        }
        case Argument(Numeric(num)) => arguments(i);
        case x => opts(x); // TODO: Deal with type coercion.
      }
    }

    mainMethod.invoke(this, methodArguments:_*);
  }

  def main(args : Array[String]){
    callWithOptions(Options.parse(args :_*));  
  }
}
