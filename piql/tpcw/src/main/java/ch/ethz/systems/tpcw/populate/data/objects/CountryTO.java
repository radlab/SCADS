package ch.ethz.systems.tpcw.populate.data.objects;

public class CountryTO extends AbstractTO {

	private int co_id;
	private String co_name;
	private double co_exchange;
	private String co_currency;

	public int getCo_id() {
		return co_id;
	}
	public void setCo_id(int co_id) {
		this.co_id = co_id;
	}
	public String getCo_name() {
		return co_name;
	}
	public void setCo_name(String co_name) {
		this.co_name = co_name;
	}
	public double getCo_exchange() {
		return co_exchange;
	}
	public void setCo_exchange(double co_exchange) {
		this.co_exchange = co_exchange;
	}
	public String getCo_currency() {
		return co_currency;
	}
	public void setCo_currency(String co_currency) {
		this.co_currency = co_currency;
	}



}
