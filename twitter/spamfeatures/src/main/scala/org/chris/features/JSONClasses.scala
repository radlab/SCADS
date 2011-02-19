package org.chris.features

import scala.collection.JavaConversions._
import scala.collection.mutable._

//////////////////////////////////////////////////////////////////////////////////////////
// Basic structure for all JSON data provided by crawler
//////////////////////////////////////////////////////////////////////////////////////////

class ParsedRecord(
    var windows : List[Window],            // Window object for main page and all pop-ups
    var num_windows : Int,

    var elapsed_time : Double,            // Time taken to load page
    var exit_status : String,            // Exist status: Forced or normal (Forced if timeout)
    var seed_md5 : String,             // MD5 of URL provided by kestrel
    var seed_url : String,             // URL provided by kestrel

    var kestrel_source : String,         // Source of URL, either email or tweet
    var tweet : Option[TweetRecord],     // Tweet, if kestrel_source = tweet, else None
    var email_source : Option[Int]        // Email source ID, if kestrel_source = email, else None
  ) extends Vectorizable {


    def fixRedirects() { 
        //windows.foreach(w => println("window has %d redirects".format(w.redirects.length)))
        //windows.foreach(w => w.redirects.sortBy { case x => (x.frameid, x.seqid) })
        windows.foreach(w => w.redirects = w.redirects.sortBy(_.frameid))

        
        //println("Fixing HTTP Redirects")
        windows.foreach(w =>
            for(i <- (w.redirects.length-1) to 0 by -1) {
              //println("working on redirect %d (type=%s,frameid=%d)".format(i,w.redirects(i).`type`,w.redirects(i).frameid))
              if(i > 0 && w.redirects(i).frameid == 0 && w.redirects(i-1).`type`.getClass == classOf[HTTPRedirect]) {
                //println("Fixed HTTP redirect!")
                w.redirects(i).`type` = w.redirects(i-1).`type`
              }
            }
        )

        //println("Fixing JS redirects")
        windows.foreach(w =>
            for(i <- 0.to(w.redirects.length-1)) {
              //println("working on redirect %d (type=%s,frameid=%d)".format(i,w.redirects(i).`type`,w.redirects(i).frameid))
              // null type = javascript!
              if(i > 0 && w.redirects(i).`type` == BrowserRedirect(null)) { 
                //println("Fixed javascript redirect!")
                w.redirects(i).`type` = BrowserRedirect("javascript")
              }
              if(i == 0) {
                w.redirects(i).`type` = BrowserRedirect(w.launcher)
              }
            }
        )

        //windows.foreach(w => println(w.redirects.map(r => "redirect type: %s".format(r.`type`)).zipWithIndex))
    }

    override def vectorize(name1: String) : HashMap[String,Any] = {
        // should take care of all the basic types
        var vec = vectorizeSimple(name1,"seed_url"::"kestrel_source"::"email_source"::Nil)

        if (!ParseFiles.modes("skip_kestrel")) {
            vec ++= vectorizeFields("kestrel_source"::Nil, name1)
        }

        if (!ParseFiles.modes("skip_email")) {
            vec ++= vectorizeFields("email_source"::Nil, name1)
        }

        if (!ParseFiles.modes("skip_tweet")) {
            tweet match {
                case Some(tw) => vec ++= tw.vectorize(name1)
                case None => 
            }
        }

        URLTool.parseURL(seed_url) match {
            case Some(s) => vec ++= s.vectorize(name1 + "_seed_url")
            case None =>
        }

        windows.map(w => w.vectorize(name1)).foreach(m => vec ++= m)

        if(ParseFiles.only_modes("only_dns"))
            return vec.filter(_._1.contains("_record_dns"))
        else if(ParseFiles.only_modes("only_ips"))
            return vec.filter(_._1.contains("_record_ips"))
        else if(ParseFiles.only_modes("only_html"))
            return vec.filter(_._1.contains("_record_html"))
        else if(ParseFiles.only_modes("only_redirects"))
            return vec.filter(_._1.contains("_record_redirect"))
        else if(ParseFiles.only_modes("only_init_url"))
            return vec.filter(_._1.contains("_record_init_url"))
        else if(ParseFiles.only_modes("only_final_url"))
            return vec.filter(_._1.contains("_record_final_url"))
        else if(ParseFiles.only_modes("only_frames"))
            return vec.filter(_._1.contains("_record_frames"))
        else if(ParseFiles.only_modes("only_sources"))
            return vec.filter(_._1.contains("_record_sources"))
        else if(ParseFiles.only_modes("only_links"))
            return vec.filter(_._1.contains("_record_links"))
        else if(ParseFiles.only_modes("only_headers"))
            return vec.filter(_._1.contains("_record_headers"))

        return vec
    }
}

class Window(
    var init_url : String,             // URL the window is seeded with
    var final_url : String,                // Final URL after all redirect resolution
    var img : Object,                    // Screenshot of page; currently unused
    var html : HashMap[String,Boolean],                 // HTML of final_url and all subframes
    var md5 : String,                    // MD5 of init_url
    var launcher : String,             // Event that launched window: seed, flash, or javascript

    var redirects : List[Redirect],        // List of redirects. Sequence ID indicates order
    var num_redirects: Int,

    var frames : List[Frame],            // List of frames encountered from init_url to final_url
    var num_frames : Int,

    var sources : List[Source],            // List of URLs contacted from init_url to final_url
    var num_sources : Int,
    
    var links : List[String],            // List of all links on a page, currently unimplemented 
    var num_links : Int,

    var plugins: List[Plugin],         // List of all plugin URLs and mime-types
    var num_plugins: Int,

    var headers : List[Header],                // Headers of all packets when resolving final_url, unimplemented
    var num_headers : Int,

    var alerts : List[String],         // Text of all alerts, prompts, and modal dialogs
    var num_alerts : Int,

    var unload : Boolean,                    // Flag if onbeforeunload event was registered

    var dns : List[DNSRecord],
    var ips : List[IPRecord]
) extends Vectorizable {

    override def vectorize(name1: String) : HashMap[String,Any] = {

        val fields = "launcher"::"num_redirects"::"num_frames"::"num_sources"::"num_links"::"num_plugins"::"num_headers"::"alerts"::"num_alerts"::"unload"::Nil
        val vec = vectorizeFields(fields, name1) 

        var finalDomain = ""
        var finalHostname = ""
        URLTool.parseURL(init_url) match {
            case Some(s) => vec ++= s.vectorize(name1 + "_init_url")
            case None =>
        }
        URLTool.parseURL(final_url) match {
            case Some(s) => 
                vec ++= s.vectorize(name1 + "_final_url")
                finalHostname = s.hostname
                s.domain match {
                    case Some(domain) => finalDomain = domain
                    case None =>
                }
            case None =>
        }

        redirects.foreach(r => vec ++= r.vectorize(name1 + "_redirect"))
        frames.foreach(r => vec ++= r.vectorize(name1 + "_frames"))
        sources.foreach(r => vec ++= r.vectorize(name1 + "_sources"))

        var matchingDomains = 0
        var matchingHostnames = 0

        // chris wrote this version
        if(num_sources > 0) {
            matchingHostnames = sources.filter(_.parsed_url match { 
                case Some(s) => s.hostname == finalHostname
                case None => false
            }).length
            vec += "real_sources_hostname_ratio" -> matchingHostnames.asInstanceOf[Float]/num_sources.asInstanceOf[Float]

            matchingDomains = sources.filter(_.parsed_url match {
                case Some(s) => s.domain == finalDomain
                case None => false
            }).length
            vec += "real_sources_domain_ratio" -> matchingDomains.asInstanceOf[Float]/num_sources.asInstanceOf[Float]
        }

        // chris wrote this version
        if(num_frames > 0) {
            matchingHostnames = frames.filter(_.parsed_url match { 
                case Some(s) => s.hostname == finalHostname
                case None => false
            }).length
            vec += "real_frames_hostname_ratio" -> matchingHostnames.asInstanceOf[Float]/num_frames.asInstanceOf[Float]

            matchingDomains = frames.filter(_.parsed_url match {
                case Some(s) => s.domain == finalDomain
                case None => false
            }).length
            vec += "real_frames_domain_ratio" -> matchingDomains.asInstanceOf[Float]/num_frames.asInstanceOf[Float]
        }


        // kurt wrote this one
        // they do the same thing
        matchingDomains = 0
        matchingHostnames = 0

        links.foreach(r => 
            URLTool.parseURL(r) match { 
                case Some(s) => vec ++= s.vectorize(name1 + "_links")
                if (s.hostname == finalHostname) {matchingHostnames += 1}
                s.domain match {
                    case Some(domain) => 
                        if (domain == finalDomain) {matchingDomains += 1}
                    case None =>
                }
                case None => 
            }
        )

        vec += "real_links_hostname_ratio" -> {
            if (num_links > 0) { 
                matchingHostnames.asInstanceOf[Float]/num_links.asInstanceOf[Float]
            }
            else { 1 }
        }

        vec += "real_links_domain_ratio" -> {
            if (num_links > 0) { 
                matchingDomains.asInstanceOf[Float]/num_links.asInstanceOf[Float]
            }
            else { 1 }
        }


        plugins.foreach(r => vec ++= r.vectorize(name1 + "_plugins"))

        if (!ParseFiles.modes("skip_headers")) {
            headers.foreach(r => vec ++= r.vectorize(name1 + "_headers"))
        }
        dns.foreach(r => vec ++= r.vectorize(name1 + "_dns"))
        ips.foreach(r => vec ++= r.vectorize(name1 + "_ips"))

        if (!ParseFiles.modes("skip_html")) {
            val maps = for(m <- html) yield makeName(m._2, name1, "html_"+m._1)
            maps.foreach(m => vec += m._1 -> m._2)
        }

        return vec
    }
}

/**
 * Window components
 */
abstract class RedirectType extends Vectorizable
case class HTTPRedirect(code: Int) extends RedirectType
{
    override def vectorize(name1: String) : HashMap[String, Any] = {
        val vec = new HashMap[String,Any]
        vec += "bit_"+name1+"_http"+code.toString -> 1
        return vec
    }
}
case class BrowserRedirect(code: String) extends RedirectType

class Redirect(
    var seqid : Int,             // Sequence ID of redirect
    var url : String,            // URL of the redirect
    var old_url : String,        // URL of the previous page/frame; may be null, identical
    var `type` : RedirectType,   // Type: 30X, meta, flash, null (== javascript)
    var frameid : Int           // Frame redirect occured for. 0 == top window
) extends Vectorizable
{
    val parsed_url = URLTool.parseURL(url)

    override def vectorize(name1: String) : HashMap[String, Any] = {
      val vec = vectorizeSimple(name1, "parsed_url"::"url"::"old_url"::"frameid"::"seqid"::Nil)

      vec ++= `type`.vectorize(name1+"_type")

      URLTool.parseURL(url) match {
        case Some(s) => vec ++= s.vectorize(name1 + "_url")
        case None =>
      }

      if(old_url != null) {
        URLTool.parseURL(old_url) match {
          case Some(s) => vec ++= s.vectorize(name1 + "_old_url")
          case None =>
        }
      }
      
      return vec
    }
} 

class Source (
    var url : String,            // URL contacted
    var status : Int,            // Status: 30X, 40X, 50X, etc
    var frameid : Int,         // Frame that launched request
    var `type` : String         // Type: browser or flash
  ) extends Vectorizable
{
    val parsed_url = URLTool.parseURL(url)

    override def vectorize(name1: String) : HashMap[String,Any] = {
        val vec = vectorizeFields("parsed_url"::"type"::Nil,name1)

        parsed_url match {
            case Some(s) => vec ++= s.vectorize(name1 + "_url")
            case None =>
        }

        return vec
    }
}

class Frame (
    var url : String,            // URL of frame
    var frameid : Int,         // id of current frame
    var parentid : Int            // id of parent, if nested frame
  ) extends Vectorizable
{
    val parsed_url = URLTool.parseURL(url)

    override def vectorize(name1: String) : HashMap[String,Any] = {
        val vec = new HashMap[String,Any]

        parsed_url match {
            case Some(s) => vec ++= s.vectorize(name1 + "_url")
            case None =>
        }

        return vec
    }
}

class Plugin(val url: String, val `type`: String) extends Vectorizable 
//    var url : String            // URL to plugin loaded
//    var `type` : String         // mime-type: flash, etc
{
  override def vectorize(name1: String) : HashMap[String,Any] = {
      val vec = vectorizeFields("type"::Nil,name1)
      
      URLTool.parseURL(url) match {
        case Some(s) => vec ++= s.vectorize(name1 + "_url")
        case None =>
      }

      return vec
    }
}

class Header(val url: String, val frameid: Int, val key_value_pairs : List[HeaderPair], val frameurl : Option[String], val referer_match : Boolean, val frameurl_null : Boolean) extends Vectorizable 
{ 
  override def vectorize(name1: String) : HashMap[String,Any] = {
    val vec = vectorizeSimple(name1)
    key_value_pairs.foreach(kv => vec ++= kv.vectorize(name1))
    return vec
  }
}
//    var url : String            // URL making outgoing request
//    var frameid : Int           // id of current frame
//    var key_value_pairs : List[HeaderPair]

class HeaderPair(val key: String, val value: List[String]) extends Vectorizable
{
    override def vectorize(name1: String) : HashMap[String,Any] = {
        val fields = "key"::Nil
        val vec = vectorizeFields(fields,name1)
        val maps = for(m <- value) yield makeName(m,name1,key)
        maps.foreach(m => vec += m._1 -> m._2)

        return vec
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
// DNS/IP components
//////////////////////////////////////////////////////////////////////////////////////////
class DNSRecord (
    var additional : List[DNSEntry],
    var answer : List[DNSEntry],
    var authority : List[DNSEntry],
    var question : List[DNSEntry],
    var mailserver_ips : java.util.HashMap[String,String],
    var nameserver_ips : java.util.HashMap[String,String],    
    var reverse_hosts : java.util.HashMap[String,String]
  ) extends Vectorizable
{

    override def vectorize(name1: String) : HashMap[String,Any] = {
      val vec = new HashMap[String,Any]

      //vec += "bit_" + name1 + "_additional_ptr_" + mergeBoolean(additional.map(a => a.ptr)) -> 1
      //vec += "bit_" + name1 + "_answer_ptr_" + mergeBoolean(additional.map(a => a.ptr)) -> 1
      //vec += "bit_" + name1 + "_authority_ptr_" + mergeBoolean(additional.map(a => a.ptr)) -> 1

      //vec += "bit_" + name1 + "_additional_bidrectional_" + mergeBoolean(additional.map(a => a.bidirectional)) -> 1
      //vec += "bit_" + name1 + "_answer_bidrectional_" + mergeBoolean(additional.map(a => a.bidirectional)) -> 1
      //vec += "bit_" + name1 + "_authority_bidirectional_" + mergeBoolean(additional.map(a => a.bidirectional)) -> 1

      additional.foreach(a => vec ++= a.vectorize(name1+"_additional"))
      answer.foreach(a => vec ++= a.vectorize(name1+"_answer"))
      authority.foreach(a => vec ++= a.vectorize(name1+"_authority"))
      question.foreach(a => vec ++= a.vectorize(name1+"_question"))

      val maps = for(m <- mailserver_ips) yield makeName(m._1, name1, "_mailserver_hosts")
      maps ++= (for(m <- nameserver_ips) yield makeName(m._1, name1, "_nameserver_hosts"))
      maps ++= (for(m <- reverse_hosts) yield makeName(m._2, name1, "_reverse_hosts"))

      if (!ParseFiles.modes("skip_individual_ips")) {
          maps ++= (for(m <- mailserver_ips) yield makeName(m._2, name1, "_mailserver_ips"))
          maps ++= (for(m <- nameserver_ips) yield makeName(m._2, name1, "_nameserver_ips"))
          maps ++= (for(m <- reverse_hosts) yield makeName(m._1, name1, "_reverse_ips"))
      }

      maps.foreach(m => vec += m._1 -> m._2)

      return vec
    }
}

//abstract class DNSSection extends Vectorizable {
//    var entries : List[DNSEntry]
//
//    override def vectorize(name1: String) : HashMap[String,Any] = {
//      val vec = HashMap[String, Any]()
//      for(e <- entries) {
//        vec ++= e.vectorize(name1 + "_entry")
//      }
//      return vec
//    }
//}

class DNSEntry (
    var domain : String,
    var ttl : String,
    var `class` : String,
    var `type` : String,
    var data : String,
    var ptr : Option[Boolean],
    var bidirectional : Option[Boolean]
  ) extends Vectorizable
{

    override def vectorize(name1:String): HashMap[String,Any] = {
        val vec = vectorizeSimple(name1+"_"+`type`, "ttl"::"type"::"ptr"::"bidirectional"::Nil)
        val fields = "ptr"::"bidirectional"::Nil
        val ptr_vec = vectorizeFields(fields,name1)

        try { 
          val ittl = makeName(ttl.toInt, name1+"_"+`type`, "ttl")
          vec += ittl._1 -> ittl._2
        } catch { 
          case ex: java.lang.NumberFormatException =>
        }

        val maps = for (m <- ptr_vec) yield makeName(true, "", m._1 + "_%s".format(m._2))
        maps.foreach(m => vec += m._1.substring(5,m._1.length) -> m._2)

        return vec
    }
}

class IPRecord (
    var asn: String,
    var city : String,
    var country_code : String,
    var ip : String,
    var latitude: Double,
    var longitude : Double,
    var prefix : String,
    var region : String
  ) extends Vectorizable
{  
    override def vectorize(name1: String) : HashMap[String,Any] = {
        // should take care of all the basic types
        val vec = vectorizeSimple(name1, "prefix"::"ip"::"latitude"::"longitude"::Nil)

        if (!ParseFiles.modes("skip_individual_ips")) {
            vec ++= vectorizeFields("ip"::Nil,name1)
        }

        if (prefix != null) {
            val prefixparts = prefix.split(' ') 
            vec += "bit_"+name1+"_prefix" -> prefixparts(0)
            val fixed = prefixparts.slice(1,prefixparts.length).map(p => makeName(p, name1, "as"))
            for(m <- fixed) {
                vec += m._1 -> m._2
            }
        }
        return vec
    }
}

//////////////////////////////////////////////////////////////////////////////////////////
// Twitter specific objects; only for kestrel_source == tweet
//////////////////////////////////////////////////////////////////////////////////////////

class TweetRecord(
    var fulltweet : FullTweetRecord, // Tweet data provided by Twitter
    var hashtags : List[String],     // Hashtags parsed from tweet
    var id : Long,                   // ID of original tweet
    var mentions : List[String],     // Mentions parsed from original tweet
    var url : String                 // URL from original tweet
  ) extends Vectorizable 
{

    override def vectorize(name1: String) : HashMap[String,Any] = {
        val fields = "hashtags"::"id"::"mentions"::Nil
        val vec = vectorizeFields(fields,name1)

        vec ++= fulltweet.vectorize(name1 + "_fulltweet")
    }
}

// Original schema from Twitter. See Twitter's documentation for more detail. 
class FullTweetRecord( 
    var created_at : Option[Long],
    var favorited : Boolean,
    var id : Long,
    var in_reply_to_screen_name : Option[String],
    var in_reply_to_status_id : Option[Long],
    var in_reply_to_user_id : Option[Long],
    var retweet_count : Option[Int],
    var retweeted : Boolean,
    var source : String,
    var text : HashMap[String,Boolean],
    var truncated : Boolean,
    var user : UserRecord
  ) extends Vectorizable {

    override def vectorize(name1:String) : HashMap[String,Any] = {
        val vec = vectorizeSimple(name1)
        vec ++= user.vectorize(name1 + "_user")
        val maps = for(m <- text) yield makeName(m._2, name1, "text_"+m._1)
        maps.foreach(m => vec += m._1 -> m._2)
        
        return vec
    }
}


// Original schema from Twitter. See Twitter's documentation for more detail. 
class UserRecord(
    var contributors_enabled : Boolean,
    var created_at : Option[Long] ,
    var description : HashMap[String,Boolean],
    var favourites_count : Int,
    //var follow_request_sent : Object,
    var followers_count : Int,
    var following : Boolean,
    var friends_count : Int,
    var geo_enabled: Boolean,
    var id: Long,
    var lang: String,
    var listed_count: Int,
    var location: List[String],
    var name: String,
    var token_name : List[String],
    //var notifications: Object,
    var profile_background_color: String,
    var profile_background_image_url: String,
    var profile_background_tile: Boolean ,
    var profile_image_url: String ,
    var profile_link_color: String,
    var profile_sidebar_border_color: String,
    var profile_sidebar_fill_color: String,
    var profile_text_color: String,
    var profile_use_background_image: Boolean ,
    var `protected`: Boolean,
    var screen_name: String,
    var token_screen_name: List[String],
    //var show_all_inline_media: Object,
    var statuses_count: Int,
    var time_zone: String,
    var url: String,
    var verified: Boolean
  ) extends Vectorizable {

    override def vectorize(name1: String) : HashMap[String,Any] = {
        val vec = vectorizeSimple(name1,"profile_image_url"::"profile_background_image_url"::"url"::Nil)

        if (profile_image_url != null) {
            URLTool.parseURL(profile_image_url) match {
                case Some(s) => vec ++= s.vectorize(name1 + "_profile_image_url")
                case None =>
            }
        }
        if (profile_background_image_url != null) {
            URLTool.parseURL(profile_background_image_url) match {
                case Some(s) => vec ++= s.vectorize(name1 + "_profile_background_image_url")
                case None =>
            }
        }
        if (url != null) {
            URLTool.parseURL(url) match {
                case Some(s) => vec ++= s.vectorize(name1 + "_url")
                case None =>
            }
        }

        val maps = for(m <- description) yield makeName(m._2, name1, "description_"+m._1)
        maps.foreach(m => vec += m._1 -> m._2)

        return vec
    }
}

