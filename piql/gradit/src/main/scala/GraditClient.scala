package edu.berkeley.cs
package scads
package piql
package gradit

import storage._
import avro.marker._

import org.apache.avro.util._

case class Word(var id: Int) extends AvroPair {
  //assign PK int to do randomness, but need to provide int when loading in words
  var word: String = _
  var definition: String = _
}

case class WordList_Word(var wordlist: String, var word: Int) extends AvroPair {
    var v = 1
}

case class WordList(var name: String) extends AvroPair {
    var v = 1
}

case class Book(var title: String) extends AvroPair {
    var v = 1
}


//call WORDcontext
case class WordContext(var word: Int, var book: String, var linenum: Int) extends AvroPair {
    // PKEY: var book: String = _ (book name)
    // PKEY: var linenum: Integer = _
    // PKEY: var word: Int = _ (word ID)
    var wordLine: String = _
}
/*
case class Context(var contextId: Int) extends AvroPair {
var word: Word = _
var book: Book = _
var wordLine: String = _
var before: String = _
var after: String = _
}
*/

class GraditClient(val cluster: ScadsCluster, executor: QueryExecutor) {
  implicit val exec = executor
  val maxResultsPerPage = 10

  // namespaces are declared to be lazy so as to allow for manual
  // createNamespace calls to happen first (and after instantiating this
  // class)

  lazy val words = cluster.getNamespace[Word]("words")
  lazy val books = cluster.getNamespace[Book]("books")
  lazy val wordcontexts = cluster.getNamespace[WordContext]("wordcontexts")
  lazy val wordlists = cluster.getNamespace[WordList]("wordlists")
  lazy val wordlist_word = cluster.getNamespace[WordList_Word]("wordlist_word")


  //contextsForWord
  // Finds all contexts for a particular word given
  
    val contextsForWord = (
        wordcontexts
            .where("wordcontexts.word".a === (0.?))
            .limit(50)
    ).toPiql
  
  //wordsFromWordlist
    val wordsFromWordList = (
        wordlist_word
            .where("wordlist_word.wordlist".a === (0.?))
            .limit(50)
            .join(words)
            .where("words.id".a === "wordlist_word.word".a)
    ).toPiql
}
