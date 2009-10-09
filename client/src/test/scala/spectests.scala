package edu.berkeley.cs.scads.test

import org.specs.runner.JUnit4


object ScadrLangSpec extends ScadsLangSpec {
    val specName = "Scadr"
    val specFile = "src/test/resources/scadr.scads" 
    val dataXMLFile = "src/test/resources/scadr_data.xml"
    val classNameMap = Map(
        "user" -> Array("name","password","email","profileData"),
        "thought" -> Array("timestamp","thought"),
        "subscription" -> Array("id","approved"),
        "topic" -> Array("id","name")
    )
    val queries = Array(
        "userByName"
    )
    val queriesXMLFile = "src/test/resources/scadr_queries.xml"
}
class ScadrLangTest extends JUnit4(ScadrLangSpec)

object ScadbookLangSpec extends ScadsLangSpec {
    val specName = "Scadbook"
    val specFile = "src/test/resources/scadbook.scads"
    val dataXMLFile = "src/test/resources/scadbook_data.xml"
    val classNameMap = Map(
        "user" -> Array("id","email","password","nickname","datejoined","active"),
        "profile" -> Array("id","birthday","hometown","sex","politicalaffiliation","interests","activities"),
        "network" -> Array("id","networkType","name"),
        "wallpost" -> Array("id","dateposted","contents"),
        "friendship" -> Array("approved"),
        "group" -> Array("id","groupType","name"),
        "poke" -> Array("id","hidden")
            )
    val queries = Array(
        "myNetworks",
        "postsOnMyWall",
        "myPostsOnOtherWalls",
        "myPostsOnUserWall",
        "myFriends",
        "myFriendsInNetwork",
        "pendingFriendRequests",
        "myGroups",
        "usersInGroup",
        "groupWall",
        "myPokedBy"
    )
    val queriesXMLFile = "src/test/resources/scadbook_queries.xml"
}
class ScadbookLangTest extends JUnit4(ScadbookLangSpec)

object ScadbayLangSpec extends ScadsLangSpec {
    val specName = "Scadbay"
    val specFile = "src/test/resources/scadbay.scads"
    val dataXMLFile = "src/test/resources/scadbay_data.xml"
    val classNameMap = Map(
        "user" -> Array("id","email","password","nickname","datejoined","active"),
        "item" -> Array("id","dateposted","duration","isFinished","title","startingprice","minimumbetraise","instantbuyprice","pictureurl","description","shortdesc"),
        "autobid" -> Array("id","maxamount"),
        "brand" -> Array("id","name"),
        "category" -> Array("id","title","cattype"),
        "bid" -> Array("id","bidamount","bidtime"),
        "comment" -> Array("id","rating","dateposted","wouldrecommend","comment","commenttype")
            )
    val queries = Array(
        "myActiveSellingItems",
        "myActiveBiddingItems",
        "myCommentsAsSeller",
        "myCommentsAsBuyer",
        "myPostedComments",
        "itemAutoBidders",
        "activeItemsByParentCategory",
        "activeItemsBySubParentCategory",
        "activeItemsByRegularCategory",
        "allParentCategories",
        "allBrandItems",
        "topItemBid"
    )
    val queriesXMLFile = "src/test/resources/scadbay_queries.xml"
}
class ScadbayLangTest extends JUnit4(ScadbayLangSpec)
