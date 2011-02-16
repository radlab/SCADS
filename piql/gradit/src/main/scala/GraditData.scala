package edu.berkeley.cs
package scads
package piql
package gradit

import java.io.File
import avro.runtime._
import org.apache.avro.file.CodecFactory

object AvroEncoder {
  def main(args: Array[String]): Unit = {
    val outFile = AvroOutFile[Word](new File("words.avro"), CodecFactory.deflateCodec(9))
    val w = new Word(1)
    w.word = "test"
    w.definition = "test"
    w.wordlist = "test"

    outFile.append(w)
    outFile.close
  }
}
