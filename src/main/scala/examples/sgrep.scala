package optional.examples

import scala.util.matching.Regex
import java.io.File

object sgrep extends optional.Application
{
  def mkRegexp(s: String): Regex = s.r
  
  def main(arg1: Regex, arg2: File) {
    for (line <- io.Source.fromFile(arg2).getLines ; m <- arg1.findFirstIn(line))
      println(line)
  }
}
