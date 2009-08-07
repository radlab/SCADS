package optional;

import scala.collection._;

case class Options(options: Map[String, String], args: List[String], rawArgs: List[String])

object Options {
  import scala.util.matching.Regex;

  private val ShortOption = new Regex("""-(\w)""")
  private val ShortSquashedOption = new Regex("""-([^-\s])(\w+)""")
  private val LongOption = new Regex("""--(\w+)""")
  private val OptionTerminator = "--"
  private val True = "true";

  /**
   * Take a list of string arguments and parse them into options.
   * Currently the dumbest option parser in the entire world, but
   * oh well.
   */
  def parse(args: String*): Options = {
    import mutable._;
    val optionsStack = new ArrayStack[String];
    val options = new OpenHashMap[String, String];
    val arguments = new ArrayBuffer[String];

    def addOption(name : String) = {
      if(optionsStack.isEmpty) options(name) = True;
      else {
        val next = optionsStack.pop;
        next match {
          case ShortOption(_) | ShortSquashedOption(_) | LongOption(_) | OptionTerminator =>
            optionsStack.push(next);
            options(name) = True;
          case x => options(name) = x;
        }
      }
    }

    optionsStack ++= args.reverse;    
    while(!optionsStack.isEmpty){
      optionsStack.pop match {
        case ShortOption(name) => addOption(name);
        case ShortSquashedOption(name, value) => options(name) = value;
        case LongOption(name) => addOption(name);
        case OptionTerminator => optionsStack.drain(arguments += _);
        case x => arguments += x; 
      }  
    }

    Options(options, arguments.toList, args.toList)
  }
}

object MakeCommand
{
  val template = """
_%s() 
{
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    opts="%s"

    if [[ ${cur} == -* ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    fi
}
complete -F _%s %s
alias %s='scala %s $*'
  """
  def mkTemplate(name: String, className: String, opts: Seq[String]): String =
    template.format(name, opts mkString " ", name, name, name, className)
  
  private def getArgNames(className: String) = {
    val clazz = Class.forName(className + "$")
    val singleton = clazz.getField("MODULE$").get()
    val m = clazz.getMethod("argumentNames")
    
    (m invoke singleton).asInstanceOf[Array[String]] map ("--" + _)
  }    
  
  def main(args: Array[String]): Unit = {
    if (args == null || args.size != 2)
      return println("Usage: mkCommand <name> <class>")
      
    val Array(scriptName, className) = args
    val opts = getArgNames(className)

    val txt = mkTemplate(scriptName, className, opts)
    val tmpfile = java.io.File.createTempFile(scriptName, "", null)
    val writer = new java.io.FileWriter(tmpfile)
    writer write txt
    writer.close()
    
    println("# run this command in bash")
    println("source " + tmpfile.getAbsolutePath())
  }
}

