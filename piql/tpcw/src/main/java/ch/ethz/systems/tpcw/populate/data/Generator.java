package ch.ethz.systems.tpcw.populate.data;

import java.util.Calendar;
import java.util.GregorianCalendar;

import ch.ethz.systems.tpcw.populate.data.objects.AbstractTO;
import ch.ethz.systems.tpcw.populate.data.objects.AddressTO;
import ch.ethz.systems.tpcw.populate.data.objects.AuthorTO;
import ch.ethz.systems.tpcw.populate.data.objects.CCXactsTO;
import ch.ethz.systems.tpcw.populate.data.objects.CountryTO;
import ch.ethz.systems.tpcw.populate.data.objects.CustomerTO;
import ch.ethz.systems.tpcw.populate.data.objects.ItemTO;
import ch.ethz.systems.tpcw.populate.data.objects.OrderTO;
import ch.ethz.systems.tpcw.populate.data.objects.OrderLineTO;

public class Generator {


	public static AbstractTO generateAddress(int addrId) {

		AddressTO address = new AddressTO();

		address.setAddr_id(addrId);
		address.setAddr_street_1(Utils.getRandomAString(15, 40));
		address.setAddr_street_2(Utils.getRandomAString(15, 40));
		address.setAddr_city(Utils.getRandomAString(4, 30));
		address.setAddr_state(Utils.getRandomAString(2, 20));
		address.setAddr_zip(Utils.getRandomAString(5, 10));
		address.setAddr_co_id(Utils.getRandomInt(1, 92));

		return address;
	}


	public static AbstractTO generateAuthor(int aId) {

		AuthorTO author = new AuthorTO();

		int year = Utils.getRandomInt(1800, 1990);
		int month = Utils.getRandomInt(0, 11);
		long date = Utils.getRandomDate(year, month);

		author.setA_id(aId);
		author.setA_fname(Utils.getRandomAString(3, 20));
		author.setA_mname(Utils.getRandomAString(1, 20));
		author.setA_lname(Utils.getRandomAString(1, 20));
		author.setA_dob(date);
		author.setA_bio(Utils.getRandomAString(125, 500));

		return author;
	}


	public static AbstractTO generateCustomer(int i, int numCustomers) {

		CustomerTO customer = new CustomerTO();

		customer.setC_id(i);
		customer.setC_uname(Utils.DigSyl(i, 0));
		customer.setC_passwd(customer.getC_uname().toLowerCase());
		customer.setC_lname(Utils.getRandomAString(8, 15));
		customer.setC_fname(Utils.getRandomAString(8, 15));
		customer.setC_addr_id(Utils.getRandomInt(1, 2 * numCustomers));
		customer.setC_phone(Utils.getRandomNString(9, 16));
		customer.setC_email(customer.getC_uname() + "@" + Utils.getRandomAString(2, 9) + ".com");

		GregorianCalendar cal = Utils.getCalendar();
		cal.add(Calendar.DAY_OF_YEAR, -1 * Utils.getRandomInt(1, 730));

		customer.setC_since(cal.getTime().getTime());

		cal.add(Calendar.DAY_OF_YEAR, Utils.getRandomInt(0, 60));
		if (cal.after(Utils.getCalendar()))
			cal = Utils.getCalendar();

		customer.setC_last_visit(cal.getTime().getTime());
		customer.setC_login(System.currentTimeMillis());

		cal = Utils.getCalendar();
		cal.add(Calendar.HOUR, 2);

		customer.setC_expiration(cal.getTime().getTime());
		customer.setC_discount((double) Utils.getRandomInt(0, 50) / 100.0);
		customer.setC_balance(0.00);
		customer.setC_ytd_pmt((double) Utils.getRandomInt(0, 99999) / 100.0);

		int year = Utils.getRandomInt(1880, 2000);
		int month = Utils.getRandomInt(0, 11);
		long birthdate = Utils.getRandomDate(year, month);

		customer.setC_birthday(birthdate);
		customer.setC_data(Utils.getRandomAString(100, 500));

		return customer;
	}


	public static AbstractTO generateCountry(int i) {

		CountryTO country = new CountryTO();

		country.setCo_id(i);
		country.setCo_name(Utils.getCountries()[i-1]);
		country.setCo_exchange(Utils.getExchanges()[i-1]);
		country.setCo_currency(Utils.getCurrencies()[i-1]);

		return country;
	}

	public static AbstractTO generateItem(int i, int numItems) {

		ItemTO item = new ItemTO();

		item.setI_id(i);
		item.setI_title(Utils.getRandomAString(14, 60));

		if (i <= (numItems / 4))
			item.setI_a_id(i);
		else
			item.setI_a_id(Utils.getRandomInt(1, numItems / 4));

		int year = Utils.getRandomInt(1930, 2000);
		int month = Utils.getRandomInt(0, 11);

		item.setI_pub_date(Utils.getRandomDate(year, month));
		item.setI_publisher(Utils.getRandomAString(14, 60));
		item.setI_subject(Utils.getSubjects()[Utils.getRandomInt(0, Utils.getSubjects().length - 1)]);
		item.setI_desc(Utils.getRandomAString(100, 500));

		item.setI_related1(Utils.getRandomInt(1, numItems));
		do {
			item.setI_related2(Utils.getRandomInt(1, numItems));
		} while (item.getI_related1() == item.getI_related2());
		do {
			item.setI_related3(Utils.getRandomInt(1, numItems));
		} while (item.getI_related3() == item.getI_related1() || item.getI_related3() == item.getI_related2());
		do {
			item.setI_related4(Utils.getRandomInt(1, numItems));
		} while (item.getI_related4() == item.getI_related3() || item.getI_related4() == item.getI_related2()
				|| item.getI_related4() == item.getI_related1());
		do {
			item.setI_related5(Utils.getRandomInt(1, numItems));
		} while (item.getI_related5() == item.getI_related4() || item.getI_related5() == item.getI_related3()
				|| item.getI_related5() == item.getI_related2() || item.getI_related5() == item.getI_related1());

		i = i / 2;
		if(i == 0) i++;

		item.setI_thumbnail(new String("img" + i % 100 + "/thumb_" + i + ".gif"));
		item.setI_image(new String("img" + i % 100 + "/image_" + i + ".gif"));
		item.setI_srp(((double) Utils.getRandomInt(100, 99999))/100.0);
		item.setI_cost(item.getI_srp() - ((((double) Utils.getRandomInt(0, 50) / 100.0)) * item.getI_srp()));

		GregorianCalendar cal = Utils.getCalendar();
		cal.add(Calendar.DAY_OF_YEAR, Utils.getRandomInt(1, 30));


		item.setI_avail(cal.getTime().getTime());
		item.setI_stock(Utils.getRandomInt(10, 30));
		item.setI_isbn(Utils.getRandomAString(13));
		item.setI_page(Utils.getRandomInt(20, 9999));
		item.setI_backing(Utils.getBackings()[Utils.getRandomInt(0, Utils.getBackings().length - 1)]);
		item.setI_dimensions(((double) Utils.getRandomInt(1, 9999) / 100.0) + "x"
				+ ((double) Utils.getRandomInt(1, 9999) / 100.0) + "x"
				+ ((double) Utils.getRandomInt(1, 9999) / 100.0));

		return item;
	}


	public static AbstractTO generateOrder(int i, int numCustomers, int numItemLines) {

		OrderTO order = new OrderTO();

		order.setO_id(i);
		order.setO_c_id(Utils.getRandomInt(1, numCustomers));

		GregorianCalendar cal = Utils.getCalendar();
		cal.add(Calendar.DAY_OF_YEAR, -1 * Utils.getRandomInt(1, 59));
		cal.add(Calendar.HOUR_OF_DAY, -1 * Utils.getRandomInt(1, 23));
		cal.add(Calendar.MINUTE, -1 * Utils.getRandomInt(1, 59));
		cal.add(Calendar.SECOND, -1 * Utils.getRandomInt(1, 59));
		cal.add(Calendar.MILLISECOND, -1 * Utils.getRandomInt(1, 999));

		order.setO_date(cal.getTime().getTime());
		order.setO_sub_total((double) Utils.getRandomInt(1000, 999999) / 100);
		order.setO_tax(order.getO_sub_total() * 0.0825);
		order.setO_total(order.getO_sub_total() + order.getO_tax() + 3.00 + numItemLines);
		order.setO_ship_type(Utils.getShipTypes()[Utils.getRandomInt(0, Utils.getShipTypes().length - 1)]);

		cal.add(Calendar.DAY_OF_YEAR, Utils.getRandomInt(0, 7));


		order.setO_ship_date(cal.getTime().getTime());
		order.setO_bill_addr_id(Utils.getRandomInt(1, 2 * numCustomers));
		order.setO_ship_addr_id(Utils.getRandomInt(1, 2 * numCustomers));
		order.setO_status(Utils.getStatusTypes()[Utils.getRandomInt(0, Utils.getStatusTypes().length - 1)]);

		return order;
	}


	public static AbstractTO generateOrderLine(int orderLineId, int orderId, int numItems) {

		OrderLineTO orderline = new OrderLineTO();

		orderline.setOl_id(orderLineId);
		orderline.setOl_o_id(orderId);
		orderline.setOl_i_id(Utils.getRandomInt(1, numItems));
		orderline.setOl_qty(Utils.getRandomInt(1, 300));
		orderline.setOl_discount((double) Utils.getRandomInt(0, 30) / 100);
		orderline.setOl_comments(Utils.getRandomAString(20, 100));

		return orderline;
	}


	public static AbstractTO generateCCXacts(int i) {

		CCXactsTO ccxacts = new CCXactsTO();

		ccxacts.setCx_o_id(i);
		ccxacts.setCx_type(Utils.getCreditCards()[Utils.getRandomInt(0, Utils.getCreditCards().length - 1)]);
		ccxacts.setCx_num(Utils.getRandomNString(16));
		ccxacts.setCx_name(Utils.getRandomAString(14, 30));

		GregorianCalendar cal = Utils.getCalendar();
		cal.add(Calendar.DAY_OF_YEAR, Utils.getRandomInt(10, 730));

		ccxacts.setCs_expiry(cal.getTime().getTime());
		ccxacts.setCx_auth_id(Utils.getRandomAString(15));
		ccxacts.setCx_co_id(Utils.getRandomInt(1, 92));

		return ccxacts;
	}

}
