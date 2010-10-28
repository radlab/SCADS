package edu.berkeley.cs.scads.piql

import net.lag.logging.Logger

import scala.util.Random
import collection.mutable.HashMap

import collection.JavaConversions._

import ch.ethz.systems.tpcw.populate.data.Utils

class TpcwWorkflow(val loader: TpcwLoader) {

  private val logger = Logger("edu.berkeley.cs.scads.piql.TpcwWorkflow")

  val random = new Random

  val subjects = Utils.getSubjects

  case class Action(val action: ActionType.Value, var nextActions : List[(Int, Action)])

  object ActionType extends Enumeration {
    type ActionType = Value

    val Home, NewProduct, BestSeller, ProductDetail, SearchRequest, SearchResult, ShoppingCart,
    CustomerReg, BuyRequest, BuyConfirm, OrderInquiry, OrderDisplay, AdminRequest, AdminConfirm = Value

  }

  object SearchResultType extends Enumeration {
    val ByAuthor, ByTitle, BySubject = Value 
  }
  val SearchTypes = Vector(SearchResultType.ByAuthor,
                           SearchResultType.ByTitle,
                           SearchResultType.BySubject)
  def randomSearchType = 
    SearchTypes(random.nextInt(SearchTypes.size))

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
        logger.debug("Home")
        val username = loader.toCustomer(random.nextInt(loader.numCustomers) + 1)
        loader.client.homeWI(username)
      }
      case Action(ActionType.NewProduct, _) => {
        logger.debug("NewProduct")
        val subject = subjects(random.nextInt(subjects.length))
        loader.client.newProductWI(subject)
      }
      case Action(ActionType.ProductDetail, _) => 
        logger.debug("ProductDetail")
        val item = loader.toItem(random.nextInt(loader.numItems) + 1)
        loader.client.productDetailWI(item)
      case Action(ActionType.SearchRequest, _) =>
        logger.debug("SearchRequest")
        // NO-OP, since this action is mostly concerned w/ presenting a user
        // w/ a form...
      case Action(ActionType.SearchResult, _) =>
        logger.debug("SearchResult")
        // pick a random search type
        randomSearchType match {
          case SearchResultType.ByAuthor =>
            val name = random.nextBoolean match {
              case false => loader.toAuthorFname(loader.toAuthor(random.nextInt(loader.numAuthors) + 1))
              case true => loader.toAuthorLname(loader.toAuthor(random.nextInt(loader.numAuthors) + 1))
            }
            loader.client.searchByAuthorWI(name)
          case SearchResultType.ByTitle =>
            // for now, just pick a random string...
            // TODO: this really should be fixed- we should
            // seed the random gen used to create random strings,
            // so we actually have a hope of matching a title.
            val title = Utils.getRandomAString(14, 60)
            loader.client.searchByTitleWI(title)
          case SearchResultType.BySubject =>
            val subject = subjects(random.nextInt(subjects.length))
            loader.client.searchBySubjectWI(subject)
        }
      case Action(tpe, _) =>
        logger.error("Not supported: " + tpe)
    }

    val rnd = random.nextInt(9999)

    val action = nextAction.nextActions.find(rnd < _._1)
    assert(action.isDefined)
    nextAction = action.get._2

 }
}
