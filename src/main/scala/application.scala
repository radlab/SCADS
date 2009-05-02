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

  private lazy val parameterTypes = 
    mainMethod.getGenericParameterTypes
  
  private val Argument = new scala.util.matching.Regex("""arg(\d+)""") 
  private object Numeric{
    def unapply(x : String) = 
      try { Some(x.toInt) }
      catch { 
        case _ : java.lang.NumberFormatException 
          => None 
      }
  }
  import java.lang.reflect.{Array => _, _};

  private val boxing = Map[Class[_], Class[_]](
    classOf[Int] -> classOf[java.lang.Integer],
    classOf[Byte] -> classOf[java.lang.Byte],
    classOf[Float] -> classOf[java.lang.Float],
    classOf[Double] -> classOf[java.lang.Double],
    classOf[Long] -> classOf[java.lang.Long],
    classOf[Char] -> classOf[java.lang.Character],
    classOf[Short] -> classOf[java.lang.Short],
    classOf[Boolean] -> classOf[java.lang.Boolean]
  )


  /**
   * Magic method to take a string and turn it into something of a given type.
   */
  private def coerceTo(value : String, tpe : Type) : AnyRef = tpe match {
    case x if x == classOf[String] => value;
    // we don't currently support other array types. This is sheer laziness.
    case clazz if clazz == classOf[Array[String]] => value.split(java.io.File.separator); 
    case (clazz : Class[_]) => boxing.getOrElse(clazz, clazz).getMethod("valueOf", classOf[String]).invoke(null, value);
    case (x : ParameterizedType) if x.getRawType == classOf[Option[_]] => Some(coerceTo(value, x.getActualTypeArguments()(0)));
  }

  private def defaultFor(tpe : Type) : AnyRef = tpe match {
    case (x : ParameterizedType) if x.getRawType == classOf[Option[_]] => None
    case x if x == classOf[Boolean] => java.lang.Boolean.FALSE;
    case (clazz : Class[_]) if clazz.isPrimitive => boxing(clazz).getMethod("valueOf", classOf[String]).invoke(null, "0");
  }

  def callWithOptions(opts : Options) = {
    val methodArguments = new Array[AnyRef](parameterTypes.length);
    import opts.{args => arguments, options};

    for(i <- 0 until methodArguments.length){
      val tpe = parameterTypes(i);
      def valueOf(x : Option[String]) = x.map(coerceTo(_, tpe)).getOrElse(defaultFor(tpe));
      methodArguments(i) = argumentNames(i) match {
        case "args" | "arguments" => {
          val x = arguments.toArray[String];
          x // work around for retarded array boxing
        }
        case Argument(Numeric(num)) => 
          if(num < arguments.length) coerceTo(arguments(num), tpe);
          else defaultFor(tpe);
        case x => valueOf(options.get(x));
      }
    }

    mainMethod.invoke(this, methodArguments:_*);
  }

  def main(args : Array[String]){
    callWithOptions(Options.parse(args :_*));  
  }
}
