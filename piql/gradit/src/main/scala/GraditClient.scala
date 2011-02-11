package edu.berkeley.cs
package scads
package piql
package gradit

import storage.ScadsCluster
import avro.marker._

import org.apache.avro.util._

case class Word(var wordid: Int) extends AvroPair {
  //assign PK int to do randomness, but need to provide int when loading in words
  var word: String = _
  var definition: String = _
  var wordlist: String = _
}

case class WordListWord(var wordlist: String, var word: Int) extends AvroPair {
    var v = 1
}

case class WordList(var name: String) extends AvroPair {
    var login: String = _
}

case class Book(var title: String) extends AvroPair {
    var v = 1
}

case class Game(var gameid: Int) extends AvroPair {
    var wordlist: String = _
    var words: String = _
    var currentword: Int = _
    var done: Int = _
}

case class GamePlayer(var login: String, var gameid: Int) extends AvroPair{
    var score: Int = 0
}

case class User(var login: String) extends AvroPair {
    var password: String = _
    var name: String = _
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
  lazy val wordlistwords = cluster.getNamespace[WordListWord]("wordlistwords")
  lazy val games = cluster.getNamespace[Game]("games")
  lazy val gameplayers = cluster.getNamespace[GamePlayer]("gameplayers")
  lazy val users = cluster.getNamespace[User]("users")
  
  // findUser
  // Primary key lookup for user
  
  var findUser = users.where("users.login".a === (0.?)).toPiql("findUser")
  
  // findGamePlayer
  // Primary key lookup for gameplayers
  
  var findGamePlayer = (
      gameplayers
        .where("gameplayers.gameid".a === (0.?))
        .where("gameplayers.login".a === (1.?))
  ).toPiql("findGamePlayer")
  
  // allWords
  // Find allWords (with cardinality constraint)
  
  /*var allWords = (
       words
         .limit(50)
  ).toPiql("allWords")*/
  
  // findWord
  // Primary key lookup for word
  
  val findWord = words.where("words.wordid".a === (0.?)).toPiql("findWord")
  
  //findWordByWord
  // Find a word by its actual string word (not wordid)
  
  val findWordByWord = (
        words
            .where("words.word".a === (0.?))
            .limit(1)
  ).toPiql("findWordByWord")
  
  // findWordList
  // Primary key lookup for wordlist
  
  val findWordList = wordlists.where("wordlists.name".a === (0.?)).toPiql("findWordList")
  
  // findWordListsByUser
  
  val findWordListsByUser = (
      wordlists
          .where("wordlists.login".a === (0.?))
          .limit(50)
  ).toPiql("findWordListsByUser")
  
  // findGame
  // Primary key lookup for game
  
  val findGame = games.where("games.gameid".a === (0.?)).toPiql("findGame")
  
  // findGameUsers
  // Return the users of a game (join through GamePlayer)
  
  val findGameUsers = new OptimizedQuery("findGameUsers",
    IndexLookupJoin(users,
		    List(AttributeValue(1,0)),
      LocalStopAfter(FixedLimit(50),
	IndexLookupJoin(gameplayers,
			List(AttributeValue(0,1),
				    AttributeValue(0,0)),
	  IndexScan(gameplayers.getOrCreateIndex("gameid" :: Nil),
		    List(ParameterValue(0)),
		    FixedLimit(50),
		    true)))),
     executor)


    /* TODO: FIX optimizer
      gameplayers
          .where("gameplayers.gameid".a === (0.?))
          .limit(50)
          .join(users)
          .where("gameplayers.login".a === "users.login".a)
  ).toPiql("findGameUsers")
  */
  
  //findGamesByUser
  // Return all games a user has
  
  val findGamesByUser = (
      gameplayers
          .where("gameplayers.login".a === (0.?))
          .limit(50)
          .join(games)
          .where("gameplayers.gameid".a === "games.gameid".a)
  ).toPiql("findGamesByUser")
  
  // contextsForWord
  // Finds all contexts for a particular word given
  
  val contextsForWord = (
        wordcontexts
            .where("wordcontexts.word".a === (0.?))
            .limit(50)
    ).toPiql("contextsForWord")
  
  // wordsFromWordlist
  
    val wordsFromWordListJoin = (
        wordlistwords
            .where("wordlistwords.wordlist".a === (0.?))
            .limit(50)
            .join(words)
            .where("words.wordid".a === "wordlistwords.word".a)
    ).toPiql("wordsFromWordListJoin")
    
    val wordsFromWordList = (
        words
            .where("words.wordlist".a === (0.?))
            .limit(50)
    ).toPiql("wordsFromWordList")
}
