package edu.berkeley.cs.scads.piql

import scala.util.Random


/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Oct 16, 2010
 * Time: 11:56:58 PM
 * To change this template use File | Settings | File Templates.
 */

class TpcwWorkflow(val client: TpcwClient, val customers : Seq[String], val authorNames : Seq[String], val subjects : Seq[String] ) {
  var random = new Random

  case class Action(val action: ActionType.Value, val value: Double)

  object ActionType extends Enumeration {
    type ActionType = Value

    val Home, NewProduct, BestSeller, ProductDetail, SearchRequest, SearchResult, ShoppingCart,
    CustomerReg, BuyRequest, BuyConfirm, OrderInquiry, OrderDisplay, AdminRequest, AdminConfirm = Value

  }


  //      Web Interaction 	Browsing Mix (WIPSb)	Shopping Mix (WIPS)	Ordering Mix (WIPSo)
  //      Home	29.00 %	16.00 %	9.12 %
  //      aNew Products	11.00 %	5.00 %	0.46 %
  //      Best Sellers	11.00 %	5.00 %	0.46 %
  //      Product Detail	21.00 %	17.00 %	12.35 %
  //      SearchRequest	12.00 %	20.00 %	14.53 %
  //
  //      Search Results	11.00 %	17.00 %	13.08 %
  //      Shopping Cart	2.00 %	11.60 %	13.53 %
  //      Customer Registration	0.82 %	3.00 %	12.86 %
  //      Buy Request	0.75 %	2.60 %	12.73 %
  //      aBuy Confirm	0.69 %	1.20 %	10.18 %
  //      aOrder Inquiry	0.30 %	0.75 %	0.25 %
  //      aOrder Display	0.25 %	0.66 %	0.22 %
  //      Admin Request	0.10 %	0.10 %	0.12 %
  //      Admin Confirm	0.09 %	0.09 %	0.11 %

  private val orderingMix = List(
    Action(ActionType.Home, 9.12),
    Action(ActionType.NewProduct, 0.46))

  //  val orderingMix = List(
  //       Action(ActionType.Home, 9.12),
  //       Action(ActionType.NewProduct, 0.46),
  //       Action(ActionType.BestSeller, 0.46),
  //       Action(ActionType.ProductDetail, 12.35),
  //       Action(ActionType.SearchRequest, 14.53),
  //       Action(ActionType.SearchResult, 13.08),
  //       Action(ActionType.ShoppingCart, 13.53),
  //       Action(ActionType.CustomerReg, 12.86),
  //       Action(ActionType.BuyRequest, 12.73),
  //       Action(ActionType.BuyConfirm, 10.18),
  //       Action(ActionType.OrderInquiry, 0.25),
  //       Action(ActionType.OrderDisplay, 0.22),
  //       Action(ActionType.AdminRequest, 0.12),
  //       Action(ActionType.AdminConfirm, 0.11))

  val orderingMixCDF = calcCDF(orderingMix)

  def executeOrderingMix() = executeMix(orderingMixCDF)

  def calcCDF(mix: List[Action]) = {
    val normFact = mix.map(_.value).reduceLeft(_ + _)
    var cummCDF = 0.0
    for (action <- mix) yield {
      cummCDF += action.value / normFact
      Action(action.action, cummCDF)
    }
  }


  def executeMix(mix: List[Action]) = {

    val rnd = random.nextDouble()
    val action = mix.find(rnd < _.value)
    assert(action.isDefined)
    action.get match {
      case Action(ActionType.Home, _) => {
        println("Home")
        val username = customers(random.nextInt(customers.length))
        client.homeWI(username)
      }
      case Action(ActionType.NewProduct, _) => {
        println("NewProduct")
        val subject = subjects(random.nextInt(subjects.length))
        client.newProductWI(subject)
      }

      case _ =>
        println("Not supported")
    }

  }
}