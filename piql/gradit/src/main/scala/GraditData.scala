package edu.berkeley.cs
package scads
package piql
package gradit

import java.io.File
import avro.runtime._
import org.apache.avro.file.CodecFactory
import scala.io.Source

object AvroEncoder {
  def main(args: Array[String]): Unit = {
    
    // --- Word ---
    
    println("Populating Words...")
    
    val outFile = AvroOutFile[Word](new File("words.avro"), CodecFactory.deflateCodec(9))
    
    for (line <- scala.io.Source.fromFile("words.graditdump").getLines())
    {
        print(".")
        var a = line.split("%|%")
        val w = new Word(a(0).toInt)
        w.word = a(2)
        w.definition = a(4)
        w.wordlist = "wl"
        outFile.append(w)
    }
    println("")
    println("")
    outFile.close
    
    println("Populating WordContexts...")
    // --- WordContext ---
    
    val outFile2 = AvroOutFile[WordContext](new File("wordcontexts.avro"), CodecFactory.deflateCodec(9))
    
    for (line <- scala.io.Source.fromFile("contexts.graditdump").getLines())
    {
        print(".")
        var a = line.split("%|%")
        val wc = new WordContext(a(0).toInt, a(2), a(4).toInt)
        wc.wordLine = a(6)
        outFile2.append(wc)
    }
    println("")
    println("")
    outFile2.close
    
    // --- WordList ---
    
    println("Populating WordLists...")
    
    val outFile3 = AvroOutFile[WordList](new File("wordlists.avro"), CodecFactory.deflateCodec(9))
    
    println(scala.io.Source.fromFile("wordlists.graditdump").getLines())
    
    for (line <- scala.io.Source.fromFile("wordlists.graditdump").getLines())
    {
        print(".")
        val wl = new WordList(line)
        wl.login = "testuser"
        outFile3.append(wl)
    }
    println("")
    println("")
    outFile3.close
    
    // --- WordListWord ---
    
    println("Populating WordListWords...")
    
    val outFile4 = AvroOutFile[WordListWord](new File("wordlistwords.avro"), CodecFactory.deflateCodec(9))
    
    for (line <- scala.io.Source.fromFile("wordlistwords.graditdump").getLines())
    {
        print(".")
        var a = line.split("%|%")
        val wlw = new WordListWord(a(0), a(2).toInt)
        outFile4.append(wlw)
    }
    println("")
    println("")
    outFile4.close
    
  }
}
