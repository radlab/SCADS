package ch.ethz.systems.tpcw.populate.data.objects;

public class AddressTO extends AbstractTO {

	private int addr_id;
	private String addr_street_1;
	private String addr_street_2;
	private String addr_state;
	private String addr_city;
	private String addr_zip;
	private int addr_co_id;

	public int getAddr_id() {
		return addr_id;
	}
	public void setAddr_id(int addr_id) {
		this.addr_id = addr_id;
	}
	public String getAddr_street_1() {
		return addr_street_1;
	}
	public void setAddr_street_1(String addr_street_1) {
		this.addr_street_1 = addr_street_1;
	}
	public String getAddr_street_2() {
		return addr_street_2;
	}
	public void setAddr_street_2(String addr_street_2) {
		this.addr_street_2 = addr_street_2;
	}
	public String getAddr_city() {
		return addr_city;
	}
	public void setAddr_city(String addr_city) {
		this.addr_city = addr_city;
	}
	public String getAddr_zip() {
		return addr_zip;
	}
	public void setAddr_zip(String addr_zip) {
		this.addr_zip = addr_zip;
	}
	public int getAddr_co_id() {
		return addr_co_id;
	}
	public void setAddr_co_id(int addr_co_id) {
		this.addr_co_id = addr_co_id;
	}
	public String getAddr_state() {
		return addr_state;
	}
	public void setAddr_state(String addr_state) {
		this.addr_state = addr_state;
	}

}
