package edu.berkeley.cs
package scads
package piql
package tpcw

import edu.berkeley.cs.scads.storage._
import edu.berkeley.cs.avro.marker._

import org.apache.avro.util._

case class Item(var I_ID: String) extends AvroPair {
  var I_TITLE: String = _
  var I_A_ID: String = _
  var I_PUB_DATE: Long = _
  var I_PUBLISHER: String = _
  var I_SUBJECT: String = _
  var I_DESC: String = _
  var I_RELATED1: Int = _
  var I_RELATED2: Int = _
  var I_RELATED3: Int = _
  var I_RELATED4: Int = _
  var I_RELATED5: Int = _
  var I_THUMBNAIL: String = _
  var I_IMAGE: String = _
  var I_SRP: Double = _
  var I_COST: Double = _
  var I_AVAIL: Long = _
  var I_STOCK: Int = _
  var ISBN: String = _
  var I_PAGE: Int = _
  var I_BACKING: String = _
  var I_DIMENSION: String = _
}

case class Country(var CO_ID: Int) extends AvroPair {
  var CO_NAME: String = _
  var CO_EXCHANGE: Double = _
  var CO_CURRENCY: String = _
}

case class Author(var A_ID: String) extends AvroPair {
  var A_FNAME: String = _
  var A_LNAME: String = _
  var A_MNAME: String = _
  var A_DOB: Long = _
  var A_BIO: String = _
}

//Different PK
case class Customer(var C_UNAME: String) extends AvroPair {
  var C_PASSWD: String = _
  var C_FNAME: String = _
  var C_LNAME: String = _
  var C_ADDR_ID: String = _
  var C_PHONE: String = _
  var C_EMAIL: String = _
  var C_SINCE: Long = _
  var C_LAST_VISIT: Long = _
  var C_LOGIN: Long = _
  var C_EXPIRATION: Long = _
  var C_DISCOUNT: Double = _
  var C_BALANCE: Double = _
  var C_YTD_PMT: Double = _
  var C_BIRTHDATE: Long = _
  var C_DATA: String = _
}

case class Order(var O_ID: String) extends AvroPair {
  var O_C_UNAME: String = _ // NOTE: replaces O_C_ID
  var O_DATE_Time: Long = _ //Change: Stores date and time
  var O_SUB_TOTAL: Double = _
  var O_TAX: Double = _
  var O_TOTAL: Double = _
  var O_SHIP_TYPE: String = _
  var O_SHIP_DATE: Long = _
  var O_BILL_ADDR_ID: String = _
  var O_SHIP_ADDR_ID: String = _
  var O_STATUS: String = _
}

case class OrderLine(var OL_O_ID: String, var OL_ID: Int) extends AvroPair {
  var OL_I_ID: String = _
  var OL_QTY: Int = _
  var OL_DISCOUNT: Double = _
  var OL_COMMENT: String = _
}

case class CcXact(var CX_O_ID: String) extends AvroPair {
  var CX_TYPE: String = _
  var CX_NUM: Int = _
  var CX_NAME: String = _
  var CX_EXPIRY: Long = _
  var CX_AUTH_ID: String = _
  var CX_XACT_AMT: Double = _
  var CX_XACT_DATE: Long = _
  var CX_CO_ID: Int = _
}

case class Address(var ADDR_ID: String) extends AvroPair {
  var ADDR_STREET1: String = _
  var ADDR_STREET2: String = _
  var ADDR_CITY: String = _
  var ADDR_STATE: String = _
  var ADDR_ZIP: String = _
  var ADDR_CO_ID: Int = _
}

/**
 * A shopping cart item is keyed on a (C_UNAME, SCL_I_ID). this means a single
 * user can only have one active shopping cart at a time
 *
 * Note: This is slightly different than the SHOPPING_ID specified by the spec
 */
case class ShoppingCartItem(var SCL_C_UNAME: String, var SCL_I_ID: String) extends AvroPair {
  var SCL_QTY: Int = _
}
