package ch.ethz.systems.tpcw.populate.data.objects;

public class OrderTO extends AbstractTO {

	private int o_id;
	private int o_c_id;
	private long o_date;
	private double o_sub_total;
	private double o_tax;
	private double o_total;
	private String o_ship_type;
	private long o_ship_date;
	private int o_bill_addr_id;
	private int o_ship_addr_id;
	private String o_status;

	public int getO_id() {
		return o_id;
	}
	public void setO_id(int o_id) {
		this.o_id = o_id;
	}
	public int getO_c_id() {
		return o_c_id;
	}
	public void setO_c_id(int o_c_id) {
		this.o_c_id = o_c_id;
	}
	public long getO_date() {
		return o_date;
	}
	public void setO_date(long o_date) {
		this.o_date = o_date;
	}
	public double getO_sub_total() {
		return o_sub_total;
	}
	public void setO_sub_total(double o_sub_total) {
		this.o_sub_total = o_sub_total;
	}
	public double getO_tax() {
		return o_tax;
	}
	public void setO_tax(double o_tax) {
		this.o_tax = o_tax;
	}
	public double getO_total() {
		return o_total;
	}
	public void setO_total(double o_total) {
		this.o_total = o_total;
	}
	public String getO_ship_type() {
		return o_ship_type;
	}
	public void setO_ship_type(String o_ship_type) {
		this.o_ship_type = o_ship_type;
	}
	public long getO_ship_date() {
		return o_ship_date;
	}
	public void setO_ship_date(long o_ship_date) {
		this.o_ship_date = o_ship_date;
	}
	public int getO_bill_addr_id() {
		return o_bill_addr_id;
	}
	public void setO_bill_addr_id(int o_bill_addr_id) {
		this.o_bill_addr_id = o_bill_addr_id;
	}
	public int getO_ship_addr_id() {
		return o_ship_addr_id;
	}
	public void setO_ship_addr_id(int o_ship_addr_id) {
		this.o_ship_addr_id = o_ship_addr_id;
	}
	public String getO_status() {
		return o_status;
	}
	public void setO_status(String o_status) {
		this.o_status = o_status;
	}


}
