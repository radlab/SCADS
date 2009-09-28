package edu.berkeley.cs.scads.test

import org.specs.runner.JUnit4


object ScadrLangSpec extends ScadsLangSpec {
    val specFile = "scadr.scads" 
    val dataXMLFile = "scadr_data.xml"
    val classNameMap = Map(
        "user" -> Array("name","password","email","profileData"),
        "thought" -> Array("timestamp","thought"),
        "subscription" -> Array("approved"),
        "topic" -> Array("name")
    )
}
class ScadrLangTest extends JUnit4(ScadrLangSpec)

object ScadbookLangSpec extends ScadsLangSpec {
    val specFile = "scadbook.scads"
    val dataXMLFile = "scadbook_data.xml"
    val classNameMap = Map(
        "user" -> Array("id","email","password","nickname","datejoined","active"),
        "profile" -> Array("id","birthday","hometown","sex","politicalaffiliation","interests","activities"),
        "network" -> Array("id","networkType","name"),
        "wallpost" -> Array("id","dateposted","contents"),
        "friendship" -> Array("approved"),
        "group" -> Array("id","groupType","name"),
        "poke" -> Array("id","hidden")
            )
}
class ScadbookLangTest extends JUnit4(ScadbookLangSpec)

object ScadbayLangSpec extends ScadsLangSpec {
    val specFile = "scadbay.scads"
    val dataXMLFile = "scadbay_data.xml"
    val classNameMap = Map(
        "user" -> Array("id","email","password","nickname","datejoined","active"),
        "item" -> Array("id","dateposted","duration","isFinished","title","startingprice","minimumbetraise","instantbuyprice","pictureurl","description","shortdesc"),
        "autobid" -> Array("id","maxamount"),
        "brand" -> Array("id","name"),
        "category" -> Array("id","title","cattype"),
        "bid" -> Array("id","bidamount","bidtime"),
        "comment" -> Array("id","rating","dateposted","wouldrecommend","comment","commenttype")
            )
}
class ScadbayLangTest extends JUnit4(ScadbayLangSpec)
