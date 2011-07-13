package edu.berkeley.cs
package twitter

import java.io.{FileInputStream, InputStreamReader, BufferedReader, File}
import avro.runtime._

object Convert {
  def main(args: Array[String]): Unit = {

    args.foreach(filename => {
      val infile = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))
      val outfile = AvroOutFile[Tweet](new File(filename + ".avro"))
      var line = infile.readLine()

      while(line != null) {
        if(line contains "text")
          line.replaceFirst("^START:", "").toAvro[Tweet].foreach(outfile.append)
        line = infile.readLine()
      }
      outfile.close()
    })
  }
}

object AverageFollowers {
  def main(args: Array[String]): Unit = {
    args.foreach(filename => {
      val infile = AvroInFile[Tweet](new File(filename)).counted
      val totalFollowers = infile.map(_.user.followers_count).reduceLeft(_ + _)
      val averageFollowers = totalFollowers / infile.count

      println(totalFollowers)
      println(averageFollowers)
    })
  }
}
