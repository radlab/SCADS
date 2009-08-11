package optional.examples

import scala.util.matching.Regex
import java.io.File

object sgrep extends optional.Application
{
  def mkRegexp(s: String): Regex = s.r
  
  register(ArgInfo('v', "invert-match", true, "Invert the sense of matching, to select non-matching lines."))
  
  def main(v: Boolean, arg1: Regex, arg2: File) {
    def cond(x: Option[_]) = if (v) x.isEmpty else x.isDefined
    for (line <- io.Source.fromFile(arg2).getLines ; if cond(arg1.findFirstIn(line)))
      println(line)
  }
}
