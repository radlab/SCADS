package org.chris.features


import java.io._
import java.text._

import scala.collection.JavaConversions._
import scala.collection.mutable._

import org.codehaus.jackson.map.{DeserializationConfig,ObjectMapper,MappingJsonFactory}
import org.codehaus.jackson._

object ParseFiles {

    val modes = new HashMap[String,Boolean]
    modes += "skip_kestrel" -> false
    modes += "skip_email" -> false
    modes += "skip_tweet" -> false
    modes += "skip_html" -> false
    modes += "skip_headers" -> false
    modes += "skip_individual_ips" -> false
    modes += "use_ngram" -> false

    val only_modes = new HashMap[String,Boolean]
    only_modes += "only_dns" -> false
    only_modes += "only_ips" -> false
    only_modes += "only_html" -> false
    only_modes += "only_redirects" -> false
    only_modes += "only_init_url" -> false
    only_modes += "only_final_url" -> false
    only_modes += "only_frames" -> false
    only_modes += "only_sources" -> false
    only_modes += "only_links" -> false
    only_modes += "only_headers" -> false

    var ngram_n = 0

    def getString(key: String)(implicit map: java.util.HashMap[String,Object]) : String = { 
        map.get(key).asInstanceOf[String]
    }
    def getBoolean(key: String)(implicit map: java.util.HashMap[String, Object]) : Boolean = { 
        map.get(key).asInstanceOf[Boolean]
    }
    def getInt(key: String)(implicit map: java.util.HashMap[String, Object]) : Int = { 
        map.get(key).asInstanceOf[java.math.BigInteger].intValue
    }
    def getLong(key: String)(implicit map: java.util.HashMap[String, Object]) : Long = { 
        map.get(key).asInstanceOf[java.math.BigInteger].longValue
    }
    def getDouble(key: String)(implicit map: java.util.HashMap[String, Object]) : Double = { 
        map.get(key).asInstanceOf[Double]
    }
    def getList(key: String)(implicit map: java.util.HashMap[String, Object]) : List[java.util.HashMap[String,Object]] = {
        map.get(key).asInstanceOf[java.util.ArrayList[java.util.HashMap[String,Object]]].toList
    }
    def getMap(key: String)(implicit map: java.util.HashMap[String, Object]) : java.util.HashMap[String, Object] = {
        map.get(key).asInstanceOf[java.util.HashMap[String,Object]]
    }
    def getListStrings(key: String)(implicit map: java.util.HashMap[String, Object]) : List[String] = {
        map.get(key).asInstanceOf[java.util.ArrayList[String]].toList
    }
    def getListObjects(key : String)(implicit map: java.util.HashMap[String, Object]) : List[Object] = {
        map.get(key).asInstanceOf[java.util.ArrayList[Object]].toList
    }

//////////////////////////////////////////////////////////////////////////////////////////
// JSON to Object methods
//////////////////////////////////////////////////////////////////////////////////////////

    def createUserRecord(implicit map: java.util.HashMap[String,Object]) : UserRecord = {
        new UserRecord (
            getBoolean("contributors_enabled"),
            timestampToLong(getString("created_at")),
            countTokens(tokenizeText(getString("description"))),
            getInt("favourites_count"),
            //null,
            getInt("followers_count"),
            getBoolean("following"),
            getInt("friends_count"),
            getBoolean("geo_enabled"),
            getLong("id"),
            getString("lang"),
            getInt("listed_count"),
            tokenize(getString("location"),","),
            getString("name"),
            tokenizeText(getString("name")),
            //null,
            getString("profile_background_color"),
            getString("profile_background_image_url"),
            getBoolean("profile_background_tile"),
            getString("profile_image_url"),
            getString("profile_link_color"),
            getString("profile_sidebar_border_color"),
            getString("profile_sidebar_fill_color"),
            getString("profile_text_color"),
            getBoolean("profile_use_background_image"),
            getBoolean("protected"),
            getString("screen_name"),
            tokenizeText(getString("screen_name")),
            //null,
            getInt("statuses_count"),
            getString("time_zone"),
            getString("url"),
            getBoolean("verified")
        )
    }

    def createTweetRecord(implicit map: java.util.HashMap[String,Object]) : FullTweetRecord = {
        new FullTweetRecord(
            timestampToLong(getString("created_at")),
            getBoolean("favorited"),
            getLong("id"),
            if (map.get("in_reply_to_screen_name") == null)
                None
            else 
                Some(getString("in_reply_to_screen_name"))
            ,
            if (map.get("in_reply_to_status_id") == null)
                None
            else
                Some(getLong("in_reply_to_status_id"))
            ,
            if (map.get("in_reply_to_user_id") == null)
                None
            else
                Some(getLong("in_reply_to_user_id"))
            ,
            if (map.get("retweet_count") == null)
                None
            else
                Some(getInt("retweet_count"))
            ,
            getBoolean("retweeted"),
            parseSource(getString("source")),
            countTokens(tokenizeText(getString("text"))),
            getBoolean("truncated"),
            createUserRecord(getMap("user")) 
        )
    }

    def createTweet(implicit map: java.util.HashMap[String,Object]) : TweetRecord = {
        new TweetRecord( 
            createTweetRecord(getMap("fulltweet")),
            getListStrings("hashtags"),
            getLong("id"),
            getListStrings("mentions"),
            getString("url")
        )
    }

    def createRedirectType(unk: Object) : RedirectType = {
        try {
            BrowserRedirect(unk.asInstanceOf[String])
        } catch {
            case ex: java.lang.ClassCastException => HTTPRedirect(unk.asInstanceOf[java.math.BigInteger].intValue)
        }
    }

    def createRedirect(implicit map: java.util.HashMap[String,Object]) : Redirect = {
        new Redirect(
            getInt("seqid"),
            getString("url"),
            getString("old_url"),
            createRedirectType(map.get("type")),
            getInt("frameid")
        )
    }

    def createSource(implicit map: java.util.HashMap[String,Object]) : Source = {
        new Source(
            getString("url"),
            getInt("status"),
            getInt("frameid"),
            getString("type")
        )
    }

    def createFrame(implicit map: java.util.HashMap[String,Object]) : Frame = {
        new Frame(
            getString("url"), 
            getInt("frameid"),
            getInt("parentid")
        )
    }

    def createPlugin(implicit map: java.util.HashMap[String,Object]) : Plugin = 
        new Plugin(getString("url"), getString("type"))

    def createHeader(implicit map : java.util.HashMap[String,Object]) : Header =
    {
        val frameuri = getString("frameuri")
        val frameurl = if (frameuri == null) None else Some(frameuri)
        //val key_value_pairs = getList("key_value_pairs").map(m =>createHeaderPair(m))

        // Somewhere in our crawler creation, key-value-pairs went from being
        // tuples to maps. Support both types.
        val key_value_pairs = {
            try {
                getList("key_value_pairs").map(m => createHeaderPair(m))
            }
            catch {
                case e: java.lang.ClassCastException =>
                    getListObjects("key_value_pairs").map(o =>
                        createHeaderPairAlt(o.asInstanceOf[java.util.ArrayList[String]].toList))
            }
        }

        val (referer_match, frameurl_null) = checkReferer(key_value_pairs,frameurl) 
        new Header(getString("url"), getInt("frameid"), key_value_pairs, frameurl, referer_match, frameurl_null)
    }

    // Parser for older file versions where Headers were lists of strings
    // Ex: ["Server", "nginx"]
    def createHeaderPairAlt(list : List[String]) : HeaderPair = {
        new HeaderPair(list(0), tokenizeHeader(list(0),list(1)))
    }

    // Parser for new file versions, where Headers are HashMaps
    // Ex: {"key": "Server", "value": "nginx"}
    def createHeaderPair(implicit map : java.util.HashMap[String,Object]) : HeaderPair = {
        var key = getString("key")
        new HeaderPair(key, tokenizeHeader(key,getString("value")))
    }

    def createWindow(implicit map : java.util.HashMap[String,Object]) : Window = {
        new Window (
            getString("init_url"),
            getString("final_url"),
            new Object(),
            countTokens(tokenizeHtml(getString("html"))),
            getString("md5"),
            getString("launcher"),

            getList("redirects").map(m => createRedirect(m)),
            getInt("num_redirects"),

            getList("frames").map(m => createFrame(m)),
            getInt("num_frames"),

            getList("sources").map(m => createSource(m)) ,
            getInt("num_sources"),
            
            getListStrings("links"),
            getInt("num_links"),

            getList("plugins").map(m => createPlugin(m)),
            getInt("num_plugins") ,

            getList("headers").map(m => createHeader(m)),
            getInt("num_headers"),

            getListStrings("alerts"), //getList("alerts").map(m => createAlert(m)),
            getInt("num_alerts"),

            getBoolean("unload"),

            getList("dns").map(m => createDNSRecord(m)),
            getList("ips").map(m => createIPRecord(m))
        )
    }

    def createDNSEntry(implicit map : java.util.HashMap[String,Object], reverse_hosts : java.util.HashMap[String,String]) : DNSEntry = {
        val domain = getString("domain")
        val data = getString("data")
        val `type` = getString("type")
        val (ptr,bidirectional) = dnsFullCircle(domain, `type`, data, reverse_hosts)
        new DNSEntry(
            domain,
            getString("ttl"),
            getString("class"),
            `type`,
            data,
            ptr,
            bidirectional
        )
    }

    def createDNSRecord(implicit map : java.util.HashMap[String,Object]) : DNSRecord = {
        val reverse_hosts = map.get("reverse_hosts").asInstanceOf[java.util.HashMap[String,String]]
        new DNSRecord(
            getList("additional").map(m => createDNSEntry(m,reverse_hosts)),
            getList("answer").map(m => createDNSEntry(m,reverse_hosts)), 
            getList("authority").map(m => createDNSEntry(m,reverse_hosts)), 
            getList("question").map(m => createDNSEntry(m,reverse_hosts)),
            map.get("mailserver_ips").asInstanceOf[java.util.HashMap[String,String]],
            map.get("nameserver_ips").asInstanceOf[java.util.HashMap[String,String]],
            reverse_hosts
        )
    }

    def createIPRecord(implicit map : java.util.HashMap[String,Object]) : IPRecord = {
        new IPRecord(
            getString("asn"),
            getString("city"),
            getString("country_code"),
            getString("ip"),
            getDouble("latitude"),
            getDouble("longitude"),
            getString("prefix"),
            getString("region")
        )
    }

    def createRecord(implicit map : java.util.HashMap[String,Object]) : ParsedRecord = {

        val kestrel_source = getString("kestrel_source")
        val rec = new ParsedRecord(
            getList("windows").map(m => createWindow(m)),
            getInt("num_windows"),
            getDouble("elapsed_time"),
            getString("exit_status"),        
            getString("seed_md5"),
            getString("seed_url"),
            kestrel_source,
            if (kestrel_source == "tweet") {
                Some(createTweet(getMap("tweet")))
            } else {
                None 
            },
            if(kestrel_source == "email")
                Some(getInt("email_source"))
            else 
                None 
        )
        rec.fixRedirects()
        return rec
    }
 
//////////////////////////////////////////////////////////////////////////////////////////
// Support methods for tokenization and complex data manipulation
//////////////////////////////////////////////////////////////////////////////////////////

    def formatTimestamp(ts: String, format : SimpleDateFormat) : Option[Long] = {
        try {
            var date = format.parse(ts)
            return Some(date.getTime())
        }
        catch {
            case e: ParseException =>
                return None
        }
    }

    def timestampToLong(ts : String) : Option[Long] = {

        // HTML: "Tue, 21 Sep 2010 08:17:06 GMT"
        // HTML: "Mon, 12 May 2008 14:27:18 GMT"
        // Twitter: "Mon Sep 20 17:37:25 +0000 2010"
        // Twitter: "Mon Nov 09 10:37:36 +0000 2009"

        var htmlts = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz")
        var twitterts = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        
        formatTimestamp(ts, htmlts) match {
            case Some(x) => return Some(x)
            case None => 
                formatTimestamp(ts,twitterts) match {
                    case Some(x) => return Some(x)
                    case None => return None
                }
        }
    }

    def tokenize(str : String, regex: String) : List[String] = {
        if (str == null) {
            return Nil
        }
        var content = regex.r.replaceAllIn(str," ")
        content = """\s+""".r.replaceAllIn(content," ")
        return content.split(' ').toList
    }

    def tokenizeText(text: String) : List[String] = {
        return tokenize(text,"[^a-zA-Z0-9]")
    }

    def tokenizeHtml(html : String) : List[String] = {
        return tokenize(html,"[^a-zA-Z0-9]")
    }

    def nGram(text : String, n : Int) : List[String] =  {

        var ngrams = new java.util.ArrayList[String]
        if (text.length < n) {
            return Nil
        }

        for (i <- 0 until text.length-n+1) {
            ngrams.add(text.substring(i,i+n))
        }

        return ngrams.toList
    }

    def parseSource(source:String) : String = {
        // <a href=\"http://www.facebook.com/twitter\" rel=\"nofollow\">Facebook</a>
        return "<.*?>".r.replaceAllIn(source,"")
    }

    def countTokens(tokens : List[String]) : HashMap[String,Boolean] = {
        var counts = new HashMap[String,Boolean] {
            override def default(key : String) = false
        }
        
        // Uncomment for real valued tokens
        //tokens.foreach {token => counts(token.toLowerCase) += 1}

        // Uncomment for binary valued tokens
        tokens.foreach {token => counts(token.toLowerCase) = true}

        return counts
    }

    def tokenizeHeader(key : String, value : String) : List[String] = {
        val token_list = key match {
            // Use special tokenizers for well known headers
            case "Server" => "/"
            case "Cookie" => return List()
            case "Set-Cookie" => return List()
            case "Referer" => " "

            // Ignore uninformative headers
            case "Date" => return List()
            case "Content-Length" => return List()
            case "Accept-Ranges" => return List()
            case "User-Agent" => return List()
            case "Warning" => return List()
            case "Expires" => return List()

            // Ignore headers added by Squid
            case "X-Cache" => return List()
            case "X-Cache-Lookup" => return List()
            case "Via" => return List()
            case "Proxy-Connection" => return List()
            case "X-Squid-Error" => return List()

            // All other custom or unspecified headers are tokenized with
            // generic separators
            case _ => "[=,]"
        }
        return tokenize(value,token_list)
    }

    // Given a DNS record (domain:google.com, data:10.0.0.1), determine whether
    // the reverse mapping of 10.0.0.1 exists, and if it matches google.com.
    def dnsFullCircle(domain : String, `type` : String, data : String, reverse_hosts : java.util.HashMap[String,String]) : (Option[Boolean],Option[Boolean]) = {
        var ptr_present = false
        var full_circle = false

        if (`type` != "A") {
            return (None,None)
        }
        
        var ptr_domain = reverse_hosts.get(data) 
        if (ptr_domain != null) {
            ptr_present = true
            if (ptr_domain == domain) {
                full_circle = true
            }
        }
        
        return (Some(ptr_present),Some(full_circle))
    }

    def checkReferer(key_value_pairs : List[HeaderPair], frameurl : Option[String]) : (Boolean, Boolean) = {
        val url = frameurl match {
            case Some(x) => x
            case None => return (false,true)
        }
        key_value_pairs.foreach(m => if (m.key == "Referer") {
                return (url==m.value(0),false)
            })

        return (false,false)
    }

    def loadArguments(args : Array[String]) {
        args.foreach(arg =>
            if (modes.isDefinedAt(arg)) {
                modes(arg) = true
            }
            else if(arg.startsWith("only_") && only_modes.isDefinedAt(arg)) {
                only_modes(arg) = true
            }
            else {
                var params = arg.split('=').toList
                if (params(0) == "ngram_n") {
                    ngram_n = params(1).toInt
                }
            }

        )
        if (modes("use_ngram") == true && ngram_n == 0) {
            println("Error:: Failed to set ngram_n. Correct usage: use_ngram ngram_n = X")
        }
    }

    def main(args: Array[String]) {
    
        loadArguments(args)
        println("---- RUNNING ON FILES -----")
        println(modes)
        println(only_modes)
        println(ngram_n)

        // Use a static directory for testing
        //val dir = new File("/data/twitter/blacklist_sample/")
        //val files = dir.listFiles
        val files = new File("/data/twitter/blacklist_sample/blacklist.3.samples")::Nil
        //val files = new File("/data/twitter/error_sample/error.1.sample")::Nil
        //val files = new File("/data/twitter/crawler/2010-09-30/14/000ee22fbf664eadf80861d80800f9e7.tweet.log")::Nil

        // Create a JSON decoder 
        var mapper = new ObjectMapper()
        mapper.getDeserializationConfig().set(DeserializationConfig.Feature.USE_BIG_INTEGER_FOR_INTS, true)

        val start_time = System.currentTimeMillis()
        val tasks = files map (filename => scala.actors.Futures.future {    

            val time = System.currentTimeMillis()

            // Read data from file
            println("Opening " + filename)
            val data = new FileInputStream(filename)
            val lineIterator = scala.io.Source.fromInputStream(data).getLines
            println("Found %d entries".format(lineIterator.length))
            val out_file = new java.io.FileWriter("blacklist.3.processed")
            lineIterator.foreach(line =>

                // Read the value as a HashMap with keys String values Object
                try {
                    implicit val obj = mapper.readValue(line, classOf[java.util.HashMap[String,Object]])
                    val newrec = createRecord(obj)
                    val vector = newrec.vectorize("record").toList
                    vector.sortBy(_._1).foreach(kv => out_file.write(("%s\t: %s\n".format(kv._1, kv._2))))
                    out_file.write("-----------------------------------------------------------------------\n")
                }
                catch {
                    case e: JsonParseException =>
                        println("JSONError:: Failed to parse %s: %s".format(filename,line))
                    case _ =>
                        println("Unknown error:: Failed to parse %s: %s".format(filename,line))
                }
            )
            val totaltime = System.currentTimeMillis()-time
            val exectime = totaltime
            totaltime
        })


        val results = tasks map (future => future.apply())
        val exectime = results.reduceLeft[Long](_+_)
        println("Real execution time %d".format(System.currentTimeMillis()-start_time))
        println("Serial execution time %d".format(exectime))
        println("Average per-file time %d".format(exectime/results.length))
        println("Parallel speedup: %d%%".format(100*exectime/(System.currentTimeMillis()-start_time)))
    }
}
