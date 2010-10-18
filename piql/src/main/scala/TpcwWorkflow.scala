package edu.berkeley.cs.scads.piql

import scala.util.Random
import collection.mutable.HashMap


/**
 * Created by IntelliJ IDEA.
 * User: tim
 * Date: Oct 16, 2010
 * Time: 11:56:58 PM
 * To change this template use File | Settings | File Templates.
 */

class TpcwWorkflow(val client: TpcwClient, val customers : Seq[String], val authorNames : Seq[String], val subjects : Seq[String] ) {
  var random = new Random

  case class Action(val action: ActionType.Value, var nextActions : List[(Int, Action)])

  object ActionType extends Enumeration {
    type ActionType = Value

    val Home, NewProduct, BestSeller, ProductDetail, SearchRequest, SearchResult, ShoppingCart,
    CustomerReg, BuyRequest, BuyConfirm, OrderInquiry, OrderDisplay, AdminRequest, AdminConfirm = Value

  }

  val actions = new HashMap[ActionType.ActionType, Action]()
  ActionType.values.foreach(a => actions += a -> new Action(a, Nil))

  actions(ActionType.AdminConfirm).nextActions = List((8348, actions(ActionType.Home)))
  println("nb of actions " + actions.size)

//The MarkovChain from TPC-W
//actions(ActionType.AdminConfirm).nextActions = List ( (8348, actions(ActionType.Home)), (9999, actions(ActionType.SearchRequest)))
//actions(ActionType.AdminRequest).nextActions = List ( (8999, actions(ActionType.AdminConfirm)), (9999, actions(ActionType.Home)))
//actions(ActionType.BestSeller).nextActions = List ( (1, actions(ActionType.Home)), (333, actions(ActionType.ProductDetail)), (9998, actions(ActionType.SearchRequest)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.BuyConfirm).nextActions = List ( (2, actions(ActionType.Home)), (9999, actions(ActionType.SearchRequest)))
//actions(ActionType.BuyRequest).nextActions = List ( (7999, actions(ActionType.BuyConfirm)), (9453, actions(ActionType.Home)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.CustomerReg).nextActions = List ( (9899, actions(ActionType.BuyRequest)), (9901, actions(ActionType.Home)), (9999, actions(ActionType.SearchRequest)))
//actions(ActionType.Home).nextActions = List ( (499, actions(ActionType.BestSeller)), (999, actions(ActionType.NewProduct)), (1269, actions(ActionType.OrderInquiry)), (1295, actions(ActionType.SearchRequest)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.NewProduct).nextActions = List ( (504, actions(ActionType.Home)), (9942, actions(ActionType.ProductDetail)), (9976, actions(ActionType.SearchRequest)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.OrderDisplay).nextActions = List ( (9939, actions(ActionType.Home)), (9999, actions(ActionType.SearchRequest)))
//actions(ActionType.OrderInquiry).nextActions = List ( (1168, actions(ActionType.Home)), (9968, actions(ActionType.OrderDisplay)), (9999, actions(ActionType.SearchRequest)))
//actions(ActionType.ProductDetail).nextActions = List ( (99, actions(ActionType.AdminRequest)), (3750, actions(ActionType.Home)), (5621, actions(ActionType.ProductDetail)), (6341, actions(ActionType.SearchRequest)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.SearchRequest).nextActions = List ( (815, actions(ActionType.Home)), (9815, actions(ActionType.SearchResult)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.SearchResult).nextActions = List ( (486, actions(ActionType.Home)), (7817, actions(ActionType.ProductDetail)), (9998, actions(ActionType.SearchRequest)), (9999, actions(ActionType.ShoppingCart)))
//actions(ActionType.ShoppingCart).nextActions = List ( (9499, actions(ActionType.CustomerReg)), (9918, actions(ActionType.Home)), (9999, actions(ActionType.ShoppingCart)))
//

  actions(ActionType.Home).nextActions = List ( (5000, actions(ActionType.Home)), (9999, actions(ActionType.NewProduct)))
  actions(ActionType.NewProduct).nextActions = List ( (9999, actions(ActionType.Home)))


  var nextAction = actions(ActionType.Home)

  def executeMix() = {
    nextAction match {
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

    val rnd = random.nextInt(9999)

    val action = nextAction.nextActions.find(rnd < _._1)
    assert(action.isDefined)
    nextAction = action.get._2

 }
}