package optional;

import com.thoughtworks.paranamer.BytecodeReadingParanamer

class DesignError(msg : String) extends Error(msg);
class InvalidCall(msg : String) extends Exception(msg);

object Util
{
  def cond[T](x: T)(f: PartialFunction[T, Boolean]) =
    (f isDefinedAt x) && f(x)
  def condOpt[T,U](x: T)(f: PartialFunction[T, U]): Option[U] =
    if (f isDefinedAt x) Some(f(x)) else None
}
import Util._

/**
 *  This trait automagically finds a main method on the object 
 *  which mixes this in and based on method names and types figures
 *  out the options it should be called with and takes care of parameter parsing
 */ 
trait Application
{
  private def designError(name : String) = throw new DesignError(name);
  private def invalidCall(name : String) = throw new InvalidCall(name);
  
  protected def usageMessage = ""

  private lazy val mainMethod =
    (getClass.getMethods filter (x => x.getName == "main" && !isRealMain(x))) match {
      case Seq()  => designError("No main method found")
      case Seq(x) => x
      case xs     =>
        designError("You seem to have multiple main methods, signatures:\n%s".format(
          (xs map (_.getParameterTypes.mkString("  (", ", ", ")")) mkString "\n")
        ))
    }

  lazy val argumentNames: Array[String] =
    (new BytecodeReadingParanamer lookupParameterNames mainMethod) map (_.replaceAll("\\$.+", ""))

  private lazy val parameterTypes = 
    mainMethod.getGenericParameterTypes
  
  private val Argument = """arg(\d+)""".r
  private object Numeric {
    def unapply(x : String) = 
      try   { Some(x.toInt) }
      catch { case _: NumberFormatException => None }
  }
  import java.lang.reflect.{Array => _, _};
  private def isRealMain(m: Method) =
    cond(m.getParameterTypes) { case Array(x) if x == classOf[Array[String]]  => true }  

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

  private object OptionType {
    def unapply(x: Any) = condOpt(x) {
      case x: ParameterizedType if x.getRawType == classOf[Option[_]] => x.getActualTypeArguments()(0)
    }
  }

  /**
   * Magic method to take a string and turn it into something of a given type.
   */
  private def coerceTo(value : String, tpe : Type) : AnyRef = tpe match {
    case x if x == classOf[String] => value;
    // we don't currently support other array types. This is sheer laziness.
    case clazz if clazz == classOf[Array[String]] => value.split(java.io.File.separator); 
    case (clazz : Class[_]) => boxing.getOrElse(clazz, clazz).getMethod("valueOf", classOf[String]).invoke(null, value);
    case OptionType(t)    => Some(coerceTo(value, t))
  }

  private def defaultFor(tpe : Type) : AnyRef = tpe match {
    case OptionType(_)    => None
    case x if x == classOf[Boolean] => java.lang.Boolean.FALSE;
    case (clazz : Class[_]) if clazz.isPrimitive => boxing(clazz).getMethod("valueOf", classOf[String]).invoke(null, "0");
  }

  private var _opts: Options = null
  lazy val opts = _opts

  def callWithOptions() = {
    import opts._
    val methodArguments = new Array[AnyRef](parameterTypes.length)

    for (i <- 0 until methodArguments.length) {
      val tpe = parameterTypes(i);
      def valueOf(x : Option[String]) =
        (x map (coerceTo(_, tpe))) getOrElse defaultFor(tpe)

      methodArguments(i) = argumentNames(i) match {
        case Argument(Numeric(num)) => 
          if (num < args.length) coerceTo(args(num), tpe);
          else defaultFor(tpe);
        case x => valueOf(options get x)
      }
    }

    mainMethod.invoke(this, methodArguments: _*)
  }
  
  def getRawArgs()  = opts.rawArgs
  def getArgs()     = opts.args
  
  def main(cmdline: Array[String]) {
    _opts = Options.parse(cmdline: _*)
    callWithOptions()
  }
}
