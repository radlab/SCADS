package optional;

import com.thoughtworks.paranamer.BytecodeReadingParanamer
import java.io.File.separator

class DesignError(msg : String) extends Error(msg);
class InvalidCall(msg : String) extends Exception(msg);

object Util
{
  val CString       = classOf[String]
  val CInteger      = classOf[java.lang.Integer]
  val CBoolean      = classOf[Boolean]
  val CArrayString  = classOf[Array[String]]
  
  def cond[T](x: T)(f: PartialFunction[T, Boolean]) =
    (f isDefinedAt x) && f(x)
  def condOpt[T,U](x: T)(f: PartialFunction[T, U]): Option[U] =
    if (f isDefinedAt x) Some(f(x)) else None
}
import Util._
import java.lang.reflect.{Array => _, _};

private object OptionType {
  def unapply(x: Any) = condOpt(x) {
    case x: ParameterizedType if x.getRawType == classOf[Option[_]] => x.getActualTypeArguments()(0)
  }
}

object MainArg {
  def apply(name: String, tpe: Type) = tpe match {
    case OptionType(t)  => OptArg(name, t)
    case _              => ReqArg(name, tpe)
  }
}
  
sealed abstract class MainArg {
  def tpe: Type
  def isOptional: Boolean
  def usage: String
  
  def tpeToString = tpe match {
    case CString      => "String"
    case CInteger     => "Int"
    case CBoolean     => "Boolean"
    case x: Class[_]  => x.getName()
    case x            => x.toString()
  }
}
case class OptArg(name: String, tpe: Type) extends MainArg {
  val isOptional = true
  def usage = "[--%s %s]".format(name, tpeToString)
}
case class ReqArg(name: String, tpe: Type) extends MainArg {
  val isOptional = false
  def usage = "<%s: %s>".format(name, tpeToString)
}

/**
 *  This trait automagically finds a main method on the object 
 *  which mixes this in and based on method names and types figures
 *  out the options it should be called with and takes care of parameter parsing
 */ 
trait Application
{
  
  private def designError(name : String) = throw new DesignError(name);
  private def invalidCall(name : String) = throw new InvalidCall(name);

  private lazy val mainMethod =
    (getClass.getMethods filter (x => x.getName == "main" && !isRealMain(x))) match {
      case Seq()  => designError("No main method found")
      case Seq(x) => x
      case xs     =>
        designError("You seem to have multiple main methods, signatures:\n%s".format(
          (xs map (_.getParameterTypes.mkString("  (", ", ", ")")) mkString "\n")
        ))
    }
  
  lazy val mainArgs = {
    val argumentNames   = (new BytecodeReadingParanamer lookupParameterNames mainMethod) map (_.replaceAll("\\$.+", ""))
    val parameterTypes  = mainMethod.getGenericParameterTypes
    
    List.map2(argumentNames.toList, parameterTypes.toList)(MainArg(_, _))
  }
  protected def programName = "program"
  protected def usageMessage = "Usage: %s %s".format(programName, mainArgs map (_.usage) mkString " ")
        
  lazy val argumentNames: Array[String] =
    (new BytecodeReadingParanamer lookupParameterNames mainMethod) map (_.replaceAll("\\$.+", ""))

  private lazy val parameterTypes = 
    mainMethod.getGenericParameterTypes
  
  private val Argument = """^arg(\d+)$""".r
  private object Numeric {
    def unapply(x : String) = 
      try   { Some(x.toInt) }
      catch { case _: NumberFormatException => None }
  }
  private def isRealMain(m: Method) = cond(m.getParameterTypes) { case Array(CArrayString) => true }

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
  private def coerceTo(tpe: Type)(value: String): AnyRef = tpe match {
    case CString          => value
    // we don't currently support other array types. This is sheer laziness.
    case CArrayString     => value split separator
    case OptionType(t)    => Some(coerceTo(t)(value))
    case clazz: Class[_]  => boxing.getOrElse(clazz, clazz).getMethod("valueOf", CString).invoke(null, value);
  }

  private def defaultFor(tpe: Type): AnyRef = tpe match {
    case CString                                    => ""
    case OptionType(_)                              => None
    case CBoolean                                   => java.lang.Boolean.FALSE
    case (clazz : Class[_]) if clazz.isPrimitive    => boxing(clazz).getMethod("valueOf", CString).invoke(null, "0")
  }

  private var _opts: Options = null
  lazy val opts = _opts

  def callWithOptions(): Unit = {
    import opts._
    val methodArguments = new Array[AnyRef](parameterTypes.length)

    for (i <- 0 until methodArguments.length) {
      val tpe = parameterTypes(i);
      def valueOf(x: Option[String]) = {
        val coerced = x map coerceTo(tpe)
        // println("%s coerces to %s".format(x, coerced))
        val res = coerced getOrElse defaultFor(tpe)
        res
      }

      methodArguments(i) = argumentNames(i) match {
        case Argument(Numeric(num)) => 
          if (num <= args.length) coerceTo(tpe)(args(num - 1))
          else mainArgs(i) match {
            case _: OptArg    => defaultFor(tpe)
            case ReqArg(x, _) => return println(usageMessage)
          }
          
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
