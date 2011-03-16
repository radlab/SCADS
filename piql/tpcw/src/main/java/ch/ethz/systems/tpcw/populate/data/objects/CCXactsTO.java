package ch.ethz.systems.tpcw.populate.data.objects;

public class CCXactsTO extends AbstractTO {

	private int cx_o_id;
	private String cx_type;
	private int cx_num;
	private String cx_name;
	private long cs_expiry;
	private String cx_auth_id;
	private double cx_xact_amt;
	private long cx_xact_date;
	private int cx_co_id;

	public int getCx_o_id() {
		return cx_o_id;
	}
	public void setCx_o_id(int cx_o_id) {
		this.cx_o_id = cx_o_id;
	}
	public String getCx_type() {
		return cx_type;
	}
	public void setCx_type(String cx_type) {
		this.cx_type = cx_type;
	}
	public int getCx_num() {
		return cx_num;
	}
	public void setCx_num(int cx_num) {
		this.cx_num = cx_num;
	}
	public String getCx_name() {
		return cx_name;
	}
	public void setCx_name(String cx_name) {
		this.cx_name = cx_name;
	}
	public long getCs_expiry() {
		return cs_expiry;
	}
	public void setCs_expiry(long cs_expiry) {
		this.cs_expiry = cs_expiry;
	}
	public String getCx_auth_id() {
		return cx_auth_id;
	}
	public void setCx_auth_id(String cx_auth_id) {
		this.cx_auth_id = cx_auth_id;
	}
	public double getCx_xact_amt() {
		return cx_xact_amt;
	}
	public void setCx_xact_amt(double cx_xact_amt) {
		this.cx_xact_amt = cx_xact_amt;
	}
	public long getCx_xact_date() {
		return cx_xact_date;
	}
	public void setCx_xact_date(long cx_xact_date) {
		this.cx_xact_date = cx_xact_date;
	}
	public int getCx_co_id() {
		return cx_co_id;
	}
	public void setCx_co_id(int cx_co_id) {
		this.cx_co_id = cx_co_id;
	}


}
