package ch.ethz.systems.tpcw.populate.data.objects;

public class AuthorTO extends AbstractTO {

	private int a_id;
	private String a_fname;
	private String a_mname;
	private String a_lname;
	private long a_dob;
	private String a_bio;

	public int getA_id() {
		return a_id;
	}
	public void setA_id(int a_id) {
		this.a_id = a_id;
	}
	public String getA_fname() {
		return a_fname;
	}
	public void setA_fname(String a_fname) {
		this.a_fname = a_fname;
	}
	public String getA_mname() {
		return a_mname;
	}
	public void setA_mname(String a_mname) {
		this.a_mname = a_mname;
	}
	public String getA_lname() {
		return a_lname;
	}
	public void setA_lname(String a_lname) {
		this.a_lname = a_lname;
	}
	public long getA_dob() {
		return a_dob;
	}
	public void setA_dob(long a_dob) {
		this.a_dob = a_dob;
	}
	public String getA_bio() {
		return a_bio;
	}
	public void setA_bio(String a_bio) {
		this.a_bio = a_bio;
	}

}
