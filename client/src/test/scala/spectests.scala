package edu.berkeley.cs.scads.test

import org.specs.runner.JUnit4


object ScadrLangSpec extends ScadsLangSpec {
    val specName = "Scadr"
    val specFile = "src/test/resources/scadr.scads"
    val dataXMLFile = "src/test/resources/scadr_data.xml"
    val classNameMap = Map(
        "user" -> Array("name","password","email","profileData"),
        "thought" -> Array("timestamp","thought","owner"),
        "thoughtref" -> Array("id","thoughtToRef","refToUser"),
        "subscription" -> Array("id","approved","following","target"),
        "topic" -> Array("id","name","hashtag")
    )
    val queries = Map(
        "user" -> Array("myThoughts","myFollowing","thoughtstream","myReferences","needsApproval"),
        "Queries" -> Array("thoughtsByHashTag","userByName","userByEmail")
    )
    val queriesXMLFile = "src/test/resources/scadr_queries.xml"
}
class ScadrLangTest extends JUnit4(ScadrLangSpec)

object ScadbookLangSpec extends ScadsLangSpec {
    val specName = "Scadbook"
    val specFile = "src/test/resources/scadbook.scads"
    val dataXMLFile = "src/test/resources/scadbook_data.xml"
    val classNameMap = Map(
        "user" -> Array("id","email","password","nickname","datejoined","active","userProfile"),
        "profile" -> Array("id","birthday","hometown","sex","politicalaffiliation","interests","activities"),
        "network" -> Array("id","networkType","name"),
        "networksubscription" -> Array("id","datejoined","networks","networktarget"),
        "wallpost" -> Array("id","dateposted","contents","wallpostsFromUser","wallpostsToUser","wallpostsToGroup"),
        "friendship" -> Array("id","approved","friendships","friendshiptarget"),
        "group" -> Array("id","groupType","name"),
        "groupsubscription" -> Array("id","datejoined","groups","grouptarget"),
        "poke" -> Array("id","hidden","pokes","poketarget")
            )
    val queries = Map(
        "user" -> Array(
            "myNetworks",
            "postsOnMyWall",
            "myPostsOnOtherWalls",
            "myPostsOnUserWall",
            "myFriends",
            "pendingFriendRequests",
            "myGroups",
            "myPokedBy"
        ),

        "group" -> Array(
            "groupWall",
            "usersInGroup"
        )
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
        "item" -> Array("id","dateposted","duration","isFinished","title","startingprice","minimumbetraise","instantbuyprice","pictureurl","description","shortdesc","myItemsSelling"),
        "itemwatch" -> Array("id","startdate","myItemsWatching","myItemsWatchingTarget"),
        "autobid" -> Array("id","maxamount","myAutoBids","autobidToItem"),
        "brand" -> Array("id","name","itemBrand"),
        "parentcategory" -> Array("title"),
        "regularcategory" -> Array("title","parentCategoryMapping"),
        "categorymap" -> Array("id","itemCategories","itemCategoriesTarget"),
        "bid" -> Array("id","bidamount","bidtime","myBids","myBidsTarget"),
        "comment" -> Array("id","rating","dateposted","wouldrecommend","comment","commenttype","commentsFromUser","commentsToUser","commentToItem")
            )
    val queries = Map(
        "user" -> Array(
            "myActiveSellingItems",
            "myActiveBiddingItems",
            "myCommentsAsSeller",
            "myCommentsAsBuyer",
            "myPostedComments",

        ),

        "item" -> Array(
            "itemAutoBidders",
            "topItemBid"
        ),

        "Queries" -> Array(
            "activeItemsByRegularCategory",
            "allParentCategories",
            "allBrandItems"
        )
    )
    val queriesXMLFile = "src/test/resources/scadbay_queries.xml"
}
class ScadbayLangTest extends JUnit4(ScadbayLangSpec)
