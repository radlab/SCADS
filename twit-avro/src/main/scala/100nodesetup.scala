package edu.berkeley.cs.scads.twitavro

import piql._

import edu.berkeley.cs.scads.comm._
import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.scads.test._
import edu.berkeley.cs.scads.comm.Conversions._
import org.apache.avro.util.Utf8

import org.apache.avro.Schema
import org.apache.zookeeper.CreateMode

object Hnodesetup {

  def main(args: Array[String]) {

    var thoughtSplits = List[Thought.KeyType]()

    var x = new Thought.KeyType
    x.owner.name="ADuran7704hnt"
    x.timestamp=1264050866
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="AllieBalling"
    x.timestamp=1264878898
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="AsifRKhan"
    x.timestamp=1264613054
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="BeefingJection"
    x.timestamp=1263946192
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="BrandonTierney"
    x.timestamp=1265077125
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="CandiceNicolePR"
    x.timestamp=1264900067
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Cindy98ER045"
    x.timestamp=1264982192
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="DJ_HOT_SAUCE"
    x.timestamp=1265001907
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Dennismik"
    x.timestamp=1265099640
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="E5P16G7"
    x.timestamp=1265577334
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="FACOVI"
    x.timestamp=1263258448
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Franny_91"
    x.timestamp=1265608092
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Glasstank"
    x.timestamp=1264971516
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="HemusAran"
    x.timestamp=1264040444
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="InfoLiner"
    x.timestamp=1264926946
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Jaicko"
    x.timestamp=1264908460
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="JoesTxGirl"
    x.timestamp=1265077630
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Kaa_Lima"
    x.timestamp=1265670752
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="KoosRoeg"
    x.timestamp=1265062840
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Leetonce"
    x.timestamp=1264088695
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="LucK3yStylez"
    x.timestamp=1265224045
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="MakeupRocks"
    x.timestamp=1263262279
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="MelRivers"
    x.timestamp=1264879581
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="MohammadAtshani"
    x.timestamp=1265015073
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Mz_Alamilla"
    x.timestamp=1264028197
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Nightmoondust"
    x.timestamp=1264286333
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="PLANEAT"
    x.timestamp=1265643675
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="PotomacWill"
    x.timestamp=1265476011
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Ralph804"
    x.timestamp=1265060820
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Roger393"
    x.timestamp=1263254407
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="SanneTerlingen"
    x.timestamp=1264966284
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="SilentManager"
    x.timestamp=1264370683
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="StratfordON"
    x.timestamp=1264183602
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="TatianaDeMaria"
    x.timestamp=1265225293
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="ThirtyTwoTwelve"
    x.timestamp=1264883067
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="UaSevastopol"
    x.timestamp=1265511751
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="Welsh4Lifey"
    x.timestamp=1264802673
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="ZeroZen"
    x.timestamp=1263254421
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="_natyc"
    x.timestamp=1264802584
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="aekituesday"
    x.timestamp=1265592264
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="aline_pac"
    x.timestamp=1264109492
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="andrendree"
    x.timestamp=1264084083
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="ariniahooy"
    x.timestamp=1264674567
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="bRittanybabex3"
    x.timestamp=1264163995
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="bethowen_am"
    x.timestamp=1264180672
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="bot_test1982"
    x.timestamp=1263927481
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="caducotavio"
    x.timestamp=1264040532
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="cellaiskandar"
    x.timestamp=1263909622
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="cindyenchant"
    x.timestamp=1265514152
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="crnrc"
    x.timestamp=1264064821
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="davecarhart"
    x.timestamp=1265098907
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="diandranadia"
    x.timestamp=1264074603
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="dracaenabot"
    x.timestamp=1264173487
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="elbapessanha"
    x.timestamp=1264039562
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="euthiagobiz"
    x.timestamp=1265685090
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="ffsamba"
    x.timestamp=1265002240
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="fun_love"
    x.timestamp=1263811904
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="gimanzz"
    x.timestamp=1265649761
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="guystuff"
    x.timestamp=1264073590
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="herolee_cn"
    x.timestamp=1263256523
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="iAbeCelentano"
    x.timestamp=1264606290
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="iloveyoubreee"
    x.timestamp=1265694161
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="itsBOSSYbitch"
    x.timestamp=1265665478
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="jbully21"
    x.timestamp=1265738458
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="joelgoh"
    x.timestamp=1264588623
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="justQuoted"
    x.timestamp=1265475603
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="katishalovesyou"
    x.timestamp=1264113384
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="kingdomofsoca"
    x.timestamp=1264458559
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="kursaal69"
    x.timestamp=1264295663
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="lehgoulart"
    x.timestamp=1265062634
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="loiedcom"
    x.timestamp=1264993593
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="m_elisssaaaa"
    x.timestamp=1265511114
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="mari_lambert"
    x.timestamp=1265603006
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="mcapanema"
    x.timestamp=1264912306
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="mikaorellana"
    x.timestamp=1265488272
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="mochira"
    x.timestamp=1265155027
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="mune_"
    x.timestamp=1263901700
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="nas_ace"
    x.timestamp=1264041450
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="nie_nie"
    x.timestamp=1265231430
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="nytimesnewsfeed"
    x.timestamp=1263869191
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="p1ug"
    x.timestamp=1264079858
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="pholk"
    x.timestamp=1263949418
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="profjqueiroz"
    x.timestamp=1265590473
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="rap39"
    x.timestamp=1265690762
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="ridgeley"
    x.timestamp=1265472075
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="rufian19"
    x.timestamp=1263866170
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="sarahstar1031"
    x.timestamp=1265163981
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="sharecatalog"
    x.timestamp=1264185913
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="sisiboriyo"
    x.timestamp=1265168806
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="spydathaking"
    x.timestamp=1264903684
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="suuuuh"
    x.timestamp=1264610099
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="tattyflores"
    x.timestamp=1264108087
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="themusicjunky"
    x.timestamp=1265618027
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="tomiya_bot"
    x.timestamp=1263816750
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="twvittper"
    x.timestamp=1265171225
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="veronikamj"
    x.timestamp=1265131886
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="wendyhendrikse"
    x.timestamp=1264939507
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="xchesisurs"
    x.timestamp=1263811977
    thoughtSplits = x :: thoughtSplits

    x = new Thought.KeyType
    x.owner.name="yorachelshin"
    x.timestamp=1265081532
    thoughtSplits = x :: thoughtSplits

    thoughtSplits = thoughtSplits.reverse

    var userSplits = List[User.KeyType]()

    var u = new User.KeyType
    u.name = "ADuran7704hnt"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "AllieBalling"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "AsifRKhan"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "BeefingJection"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "BrandonTierney"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "CandiceNicolePR"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Cindy98ER045"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "DJ_HOT_SAUCE"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Dennismik"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "E5P16G7"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "FACOVI"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Franny_91"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Glasstank"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "HemusAran"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "InfoLiner"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Jaicko"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "JoesTxGirl"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Kaa_Lima"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "KoosRoeg"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Leetonce"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "LucK3yStylez"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "MakeupRocks"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "MelRivers"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "MohammadAtshani"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Mz_Alamilla"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Nightmoondust"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "PLANEAT"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "PotomacWill"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Ralph804"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Roger393"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "SanneTerlingen"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "SilentManager"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "StratfordON"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "TatianaDeMaria"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "ThirtyTwoTwelve"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "UaSevastopol"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "Welsh4Lifey"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "ZeroZen"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "_natyc"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "aekituesday"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "aline_pac"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "andrendree"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "ariniahooy"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "bRittanybabex3"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "bethowen_am"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "bot_test1982"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "caducotavio"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "cellaiskandar"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "cindyenchant"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "crnrc"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "davecarhart"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "diandranadia"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "dracaenabot"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "elbapessanha"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "euthiagobiz"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "ffsamba"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "fun_love"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "gimanzz"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "guystuff"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "herolee_cn"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "iAbeCelentano"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "iloveyoubreee"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "itsBOSSYbitch"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "jbully21"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "joelgoh"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "justQuoted"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "katishalovesyou"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "kingdomofsoca"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "kursaal69"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "lehgoulart"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "loiedcom"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "m_elisssaaaa"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "mari_lambert"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "mcapanema"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "mikaorellana"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "mochira"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "mune_"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "nas_ace"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "nie_nie"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "nytimesnewsfeed"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "p1ug"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "pholk"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "profjqueiroz"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "rap39"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "ridgeley"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "rufian19"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "sarahstar1031"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "sharecatalog"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "sisiboriyo"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "spydathaking"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "suuuuh"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "tattyflores"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "themusicjunky"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "tomiya_bot"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "twvittper"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "veronikamj"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "wendyhendrikse"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "xchesisurs"
    userSplits = u :: userSplits

    u =  new User.KeyType
    u.name = "yorachelshin"
    userSplits = u :: userSplits

    userSplits = userSplits.reverse

    val cluster = new ScadsCluster(new ZooKeeperProxy(args(0)).root("scads"))
    val ns1 = cluster.createAndConfigureNamespace[User.KeyType,User.ValueType]("ent_User",userSplits)
    val ns2 = cluster.createAndConfigureNamespace[Thought.KeyType,Thought.ValueType]("ent_Thought",thoughtSplits)
  }
}
