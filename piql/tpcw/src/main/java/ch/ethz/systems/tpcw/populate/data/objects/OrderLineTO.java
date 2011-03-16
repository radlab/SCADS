package ch.ethz.systems.tpcw.populate.data.objects;

public class OrderLineTO extends AbstractTO {

	private int ol_id;
	private int ol_o_id;
	private int ol_i_id;
	private int ol_qty;
	private double ol_discount;
	private String ol_comments;

	public int getOl_id() {
		return ol_id;
	}
	public void setOl_id(int ol_id) {
		this.ol_id = ol_id;
	}
	public int getOl_o_id() {
		return ol_o_id;
	}
	public void setOl_o_id(int ol_o_id) {
		this.ol_o_id = ol_o_id;
	}
	public int getOl_i_id() {
		return ol_i_id;
	}
	public void setOl_i_id(int ol_i_id) {
		this.ol_i_id = ol_i_id;
	}
	public int getOl_qty() {
		return ol_qty;
	}
	public void setOl_qty(int ol_qty) {
		this.ol_qty = ol_qty;
	}
	public double getOl_discount() {
		return ol_discount;
	}
	public void setOl_discount(double ol_discount) {
		this.ol_discount = ol_discount;
	}
	public String getOl_comments() {
		return ol_comments;
	}
	public void setOl_comments(String ol_comments) {
		this.ol_comments = ol_comments;
	}

}
