package org.chris.features

import java.io._
import java.net._
import java.lang.Integer
import java.lang.NumberFormatException
import java.lang.Math
import scala.collection.mutable._

/////////////////////////////////////////////////////////////////////
// URL vectorization and canonicalization tools
/////////////////////////////////////////////////////////////////////
class ParsedURL (
    val hostname : String,
    val path : Option[String],
    val params : Option[String],
    val tag : Option[String],

    val domain : Option[String],
    val tld : Option[String],
    val ip : Option[String],

    val hostname_tokens : List[String],
    val path_tokens : List[String],
    val param_tokens : List[String],
    val tag_tokens : List[String],

    val host_obfuscated : Boolean,
    val path_obfuscated : Option[Boolean],
    val params_obfuscated : Option[Boolean],
    val tag_obfuscated: Option[Boolean],
    val ip_obfuscated : Option[Boolean]
) extends Vectorizable 
{
    override def vectorize(name1 : String) : HashMap[String,Any] =
    {
        val fields = "ip_obfuscated"::"host_obfuscated"::"params_obfuscated"::"tag_obfuscated"::"path_obfuscated"::Nil
        val vec = vectorizeSimple(name1, "hostname"::"path"::"tag"::"params"::fields)
        val obf_vec = vectorizeFields(fields, name1)
        val maps = for (m <- obf_vec) yield makeName(true, "", m._1 + "_%s".format(m._2))
        maps.foreach(m => vec += m._1.substring(5,m._1.length) -> m._2)

        // new meta-features that are calculated by looking at existing features
        domain match { 
          case Some(_) =>
            val num_tokens = makeName(hostname_tokens.length, name1, "num_hostname_tokens") 
            vec += num_tokens._1 -> num_tokens._2
            val three = makeName(hostname_tokens.length > 3, name1, "num_hostname_tokens_gt3") 
            vec += three._1 -> three._2
            val six = makeName(hostname_tokens.length > 6, name1, "num_hostname_tokens_gt6") 
            vec += six._1 -> six._2
          case None =>
        }

        path match {
          case Some(s) =>
            val pathlen = makeName(s.length, name1, "path_len")
            vec += pathlen._1 -> pathlen._2
            val pathtok = makeName(path_tokens.length, name1, "num_path_tokens")
            vec += pathtok._1 -> pathtok._2
          case None =>
        }

        params match {
          case Some(s) =>
            val paramlen = makeName(s.length, name1, "param_len")
            vec += paramlen._1 -> paramlen._2
            val paramtok = makeName(param_tokens.length, name1, "num_param_tokens")
            vec += paramtok._1 -> paramtok._2
          case None =>
        }

        val urlstr = toString()
        val urllen = makeName(urlstr.length, name1, "len")
        vec += urllen._1 -> urllen._2
        val urllen20 = makeName(urlstr.length > 20, name1, "len_gt20")
        vec += urllen20._1 -> urllen20._2
        val urllen40 = makeName(urlstr.length > 40, name1, "len_gt40")
        vec += urllen40._1 -> urllen40._2
        val urllen80 = makeName(urlstr.length > 80, name1, "len_gt80")
        vec += urllen80._1 -> urllen80._2

        return vec
    }

    override def equals(other: Any) = other match {
        case that: ParsedURL => (this.hostname == that.hostname) 
            //((this.path, that.path) match { 
            //    case (Some(p1), Some(p2)) => (p1.toLowerCase() == p2.toLowerCase())
            //    case (_, _) => false
            //}))
        case _ => false
    }

    override def toString() : String = { 
        (path, params) match { 
            case (None, None) => "%s".format(hostname)
            case (Some(x), None) => "%s/%s".format(hostname, x)
            case (None, Some(x)) => "%s/?%s".format(hostname, x)
            case (Some(x), Some(y)) => "%s/%s?%s".format(hostname, x, y)
        }
    }
    def ostr(t: Option[String]) : String = { if(t == None) "" else t.get }

    def pretty() : String = { 
        var str = "%s\n".format(toString())
        str += "  hostname: %s (%s) (obfus: %s)\n".format(hostname, hostname_tokens, host_obfuscated)
        if(path != None)
          str += "  path:     %s (%s) (obfus: %s)\n".format(path.get, path_tokens, path_obfuscated.get)
        if(tag != None)
          str += "  tag:      %s (%s) (obfus: %s)\n".format(tag.get, tag_tokens, tag_obfuscated.get)
        if(params != None)
          str += "  params:   %s (%s) (obfus: %s)\n".format(params.get, param_tokens, params_obfuscated.get)
        if(domain != None) {
          str += "  domain:   %s\n".format(domain.get)
        } else {
          str += "  ip:       %s (obfus: %s)\n".format(ip.get, ip_obfuscated.get)
        }
        if(tld != None) {
          str += "  tld:      %s".format(tld.get)
        }
        str
    }
  //params:   %s (%s) (%s)
  //tld:      %s
  //ip:       %s"""
} 

object URLTool {
    
  val tldFile = this.getClass.getClassLoader.getResourceAsStream("effective_tld_names.txt")
  println("tldFile: " + tldFile)
  val tlds = new scala.collection.mutable.HashSet[String]
    scala.io.Source.fromInputStream(tldFile, "utf-8").getLines.foreach(m => if (m.trim != "") {tlds += m.trim.toLowerCase}) 
    
    ///////////////////////////////////////////////////////////////////////
    // Parse a URL, returning a tokenized version of the URL string
    // and information regarding obfuscation

    def parseURL(_url : String) : Option[ParsedURL] = {
   
        var url = _url
        //println(url)

        // Check URL begins with a protocol, else default to http
        if ("^[^:]*?://".r.findFirstIn(url).isEmpty) {
            //println("Added protocol")
            url = "http://" + url
        }
        
        // Remove all cache messages from URLs
        if (!"^wyciwyg".r.findFirstIn(url).isEmpty) {
            //println("Was wyciwyg")
            url = url.replace("^wyciwyg://[^/]*/","")
        }

        url = url.trim().toLowerCase()

        //println(url)

        var parsed_url = nativeParseURL(url) match { 
          case None => return None
          case Some(res) => res
        }

        var (path,path_obfuscated) = deobfuscatePath(parsed_url.getPath())
        var (params,params_encoded) = decode(parsed_url.getQuery())
        var (tag,tag_encoded) = decode(parsed_url.getRef())
  

        // If no hostname, dont proceed
        var (hostname, host_obfuscated) = deobfuscateHost(parsed_url.getHost()) match {  
            case (Some(h), Some(ho)) => (h.replace("^www\\.",""), ho)
            case (None, None) => return None
            case (Some(h), None) => return None
            case (None, Some(ho)) => return None
        }

        // Parse hostname as either a domain or an IP
        // If neither work, dont proceed
        var (domain,tld) = parseDomain(hostname)
        var (ip,ip_obfuscated) = 
            domain match {
                case None => parseIP(hostname)
                case Some(_) => (None, None)
            }

        if (domain == None && ip == None) {
            return None
        }

        // Return final abstract object for parsed URL
        Some(new ParsedURL(
            if (ip == None) hostname else ip.get,
            path,
            params,
            tag,
            domain,
            tld,
            ip,
            if (domain == None) List(ip.get) else tokenizeText(Some(hostname)),
            tokenizeText(path),
            tokenizeText(params),
            tokenizeText(tag),
            host_obfuscated,
            path_obfuscated,
            params_encoded,
            tag_encoded,
            ip_obfuscated
        ))
    }

    def nativeParseURL(url : String) : Option[URL] = {
        try {
            Some(new URL(url))
        } catch {
            case e: MalformedURLException =>
            None
        }
    }

    def parseDomain(host : String) : (Option[String],Option[String]) = {

        val urlElements = host.split('.').toList
        val range = urlElements.length to 0 by -1
        val end = urlElements.length

        for (i <- range) {
            var lastIElements = urlElements.slice(i,end)
            var candidate = lastIElements.mkString(".")
            var wildcardCandidate = "*." + lastIElements.slice(1,lastIElements.length).mkString(".")
            var exceptionCandidate = "!"+candidate
            
            if (tlds.contains(exceptionCandidate)) {
                val elements = urlElements.slice(i,end)
                val tld = elements.slice(1,elements.length).mkString(".")
                val domain = elements.mkString(".")
                return (Some(domain),Some(tld))
            }
            if (tlds.contains(candidate) || tlds.contains(wildcardCandidate)) {
                val elements = urlElements.slice(i-1,end)
                val tld = elements.slice(1,elements.length).mkString(".")
                val domain = elements.mkString(".")
                return (Some(domain),Some(tld))
            }
        }
        return (None,None)
    }

    def parseIP(_ip: String) : (Option[String], Option[Boolean]) = {

        var ip = _ip

        //println("parseIP called for %s".format(ip))

        if ("[^a-f0-9\\.x]".r.findFirstIn(ip) != None) {
            //println("Invalid characters for ip")
            return (None,None)
        }

        var octets = ip.split('.').toList
        var decimal_format = 0L

        if (octets.length > 4) {
            return (None,None)
        }

        for (i <- 0 to octets.length-1) {
            var d = 0
            var octet = octets(i).toString

            // Supported formats: octal, hex, and decimal
            // Any number that begins with 0x is HEX
            // Any number that begins with 0 is OCTAL
            // Any other numbers are considered DECIMAL
            try {
                if ("^0x".r.findFirstIn(octet) != None) {
                    d = Integer.parseInt(octet.substring(2,octet.length),16)
                }
                else if ("^0".r.findFirstIn(octet) != None) {
                    d = Integer.parseInt(octet,8)
                }
                else if ("[^0-9]".r.findFirstIn(octet) == None) {
                    d = Integer.parseInt(octet,10)
                }
                else {
                    //println("Octet didnt match any format: %s".format(octet))
                    return (None,None)
                }
            } catch {
                case e: NumberFormatException =>
                    //println("Number conversion error: %s".format(octet))
                    return (None,None)
            }

            if (i == octets.length-1) {
                decimal_format += d
            }
            else {
                d = d % 256
                decimal_format += (d*Math.pow(256,3-i)).toInt
            }
        }

        var was_obfuscated = false
        var ipString = ipDecimalToString(decimal_format)
        if (ipString != ip) {
            was_obfuscated = true
        }

        //println(ipString)
        //println(was_obfuscated)
        return(Some(ipString),Some(was_obfuscated))
    }

    def ipDecimalToString(_decimal_format : Long) : (String) = {
        var decimal_format = _decimal_format & (Math.pow(2,32) -1).toLong
        var octets = ListBuffer(0,0,0,0)

        for (i <- 0 to 3) {
            var mask = 255L << 8*i
            var octet = (decimal_format & mask) >> 8*i
            octets(i) = octet.toInt
        }

        return octets.toList.reverse.mkString(".")
    }

    def tokenize(str : String, regex: String) : List[String] = {
        if (str == null) {
            return Nil
        }
        var content = regex.r.replaceAllIn(str," ")
        content = """\s+""".r.replaceAllIn(content," ")
        return content.split(' ').toList
    }

    def tokenizeText(text: Option[String]) : List[String] = {
        text match {
            case Some(x) => 
            {
                if(ParseFiles.modes("use_ngram") == false) {
                    return tokenize(x,"[^a-zA-Z0-9]")
                }
                else {
                    return ParseFiles.nGram(x,ParseFiles.ngram_n)
                }
            }
            case None => return Nil
        }
    }

    ///////////////////////////////////////////////////////////////////////////
    // Decode a URL in UTF-8 until it can no longer be decoded. Returns decoded
    // string and flag indicated whether string was encoded.

    def decode(_text: String) : (Option[String], Option[Boolean]) = {

        if (_text == null) {
            return (None,None)
        }

        var text = _text
        var decoded_text = ""
        var was_encoded = false
    
        while (true) {
            try {
                decoded_text = URLDecoder.decode(text,"UTF-8")
                if (text == decoded_text) {
                    return (Some(text), Some(was_encoded))
                }
                else {
                    text = decoded_text
                    was_encoded = true
                }
            }

            // XXX: FIXME
            // Java's implementation of URLdecode is different from Javascript
            // unquote. It does not fail gracefully for malformed URLs. Ignore
            // the exception, and return the best unquote we could get.
            catch {
                case e: IllegalArgumentException =>
                    //println("Failed to decode text: %s (%s)".format(text,_text))
                    return (Some(text),Some(was_encoded))
            }
        }

        return(None,None)
    }

    def deobfuscateHost(_host: String) : (Option[String],Option[Boolean]) = {

        if (_host == null) {
            return (None,None)
        }

        var host = _host
        var was_obfuscated = false
        var collapsed_host = ""
        
        collapsed_host = "\\.*$".r.replaceAllIn(host,"")
        if (collapsed_host != host) {
            was_obfuscated = true
        }
        host = collapsed_host
        collapsed_host = "^\\.*".r.replaceAllIn(host,"")
        if (collapsed_host != host) {
            was_obfuscated = true
        }
        host = collapsed_host

        return (Some(host),Some(was_obfuscated))
    }

    def deobfuscatePath(_path: String) : (Option[String], Option[Boolean]) = {

        if (_path == null) {
            return (None,None)
        }

        var path = "/".r.replaceFirstIn(_path, "")
        var was_obfuscated = false

        // Remove single . directory control
        var is_minimized = false
        var collapsed_path = ""
        while (!is_minimized) {
            collapsed_path = "/\\./".r.replaceAllIn(path,"/")
            if (collapsed_path == path) {
                is_minimized = true
            }
            else {
                path = collapsed_path
                was_obfuscated = true
            }
        }

        // Remove .. directory controls. Results in removing previous folder, if
        // it exists.
        is_minimized = false
        collapsed_path = ""
        while (!is_minimized) {
            collapsed_path = "/([^/]*?)/\\.\\./".r.replaceAllIn(path,"/")
            if (collapsed_path == path) {
                is_minimized = true
            }
            else {
                path = collapsed_path
                was_obfuscated = true
            }
        }

        // Remove all double //, replacing with single /
        is_minimized = false
        collapsed_path = ""
        while (!is_minimized) {
            collapsed_path = "//".r.replaceAllIn(path,"/")
            if (collapsed_path == path) {
                is_minimized = true
            }
            else {
                path = collapsed_path
                was_obfuscated = true
            }
        }

        return (Some(path),Some(was_obfuscated))
    }

    def main(args : Array[String]) {
        var url = "http://..maps.google.com/1/2/.././3/4.html?a=b&c=d#ohno%20%20%20rar"
        parseURL(url) match { 
            case None => println("parseURL(%s) is None".format(url))
            case Some(pu) => 
              println(pu.pretty())
        }
        parseURL("https://console.aws.amazon.com/ec2/home#c=EC2&s=Images") match {
            case None => println("parseURL(%s) is None".format(url))
            case Some(pu) => 
              println(pu.pretty())
        }
        parseURL("http://2088988525/") match {
            case None => println("parseURL is None")
            case Some(pu) => 
              println(pu.pretty())
        }
        parseURL("http://167772161/") match {
            case None => println("parseURL is None")
            case Some(pu) => 
              println(pu.pretty())
        }
    }
}
