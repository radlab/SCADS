package edu.berkeley.cs
package scads
package piql
package gradit

import storage.TestScalaEngine

object TestScadr {
  def main(args: Array[String]): Unit = {
    val client = new GraditClient(TestScalaEngine.newScadsCluster(), new SimpleExecutor)

    val word = new Word(0)
    word.word = "test"
    word.definition = "to see if piql works"
    client.words.put(word.key, word.value)

    val context = new WordContext(0, "TestBook", 1)
    context.wordLine = "this program is a test of PIQL"
    client.wordcontexts.put(context.key, context.value)

    client.contextsForWord(0).foreach(println)

    //Since we are running a test scads server we need to force exit or it will hang here.
    System.exit(0)
  }
}
