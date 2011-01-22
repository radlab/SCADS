package ch.ethz.systems.tpcw.populate.data;

import java.util.GregorianCalendar;
import java.util.Random;

public class Utils {

	private static Random rand = new Random();
	private static GregorianCalendar cal = new GregorianCalendar();

	private static final String[] COUNTRIES = { "United States", "United Kingdom", "Canada",
			"Germany", "France", "Japan", "Netherlands", "Italy",
			"Switzerland", "Australia", "Algeria", "Argentina", "Armenia",
			"Austria", "Azerbaijan", "Bahamas", "Bahrain", "Bangla Desh",
			"Barbados", "Belarus", "Belgium", "Bermuda", "Bolivia",
			"Botswana", "Brazil", "Bulgaria", "Cayman Islands", "Chad",
			"Chile", "China", "Christmas Island", "Colombia", "Croatia",
			"Cuba", "Cyprus", "Czech Republic", "Denmark",
			"Dominican Republic", "Eastern Caribbean", "Ecuador", "Egypt",
			"El Salvador", "Estonia", "Ethiopia", "Falkland Island",
			"Faroe Island", "Fiji", "Finland", "Gabon", "Gibraltar",
			"Greece", "Guam", "Hong Kong", "Hungary", "Iceland", "India",
			"Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Jamaica",
			"Jordan", "Kazakhstan", "Kuwait", "Lebanon", "Luxembourg",
			"Malaysia", "Mexico", "Mauritius", "New Zealand", "Norway",
			"Pakistan", "Philippines", "Poland", "Portugal", "Romania",
			"Russia", "Saudi Arabia", "Singapore", "Slovakia",
			"South Africa", "South Korea", "Spain", "Sudan", "Sweden",
			"Taiwan", "Thailand", "Trinidad", "Turkey", "Venezuela",
			"Zambia" };

	private static final double[] EXCHANGES = { 1, .625461, 1.46712, 1.86125, 6.24238, 121.907,
			2.09715, 1842.64, 1.51645, 1.54208, 65.3851, 0.998, 540.92,
			13.0949, 3977, 1, .3757, 48.65, 2, 248000, 38.3892, 1, 5.74,
			4.7304, 1.71, 1846, .8282, 627.1999, 494.2, 8.278, 1.5391,
			1677, 7.3044, 23, .543, 36.0127, 7.0707, 15.8, 2.7, 9600,
			3.33771, 8.7, 14.9912, 7.7, .6255, 7.124, 1.9724, 5.65822,
			627.1999, .6255, 309.214, 1, 7.75473, 237.23, 74.147, 42.75,
			8100, 3000, .3083, .749481, 4.12, 37.4, 0.708, 150, .3062,
			1502, 38.3892, 3.8, 9.6287, 25.245, 1.87539, 7.83101, 52,
			37.8501, 3.9525, 190.788, 15180.2, 24.43, 3.7501, 1.72929,
			43.9642, 6.25845, 1190.15, 158.34, 5.282, 8.54477, 32.77,
			37.1414, 6.1764, 401500, 596, 2447.7 };

	private static final String[] CURRENCIES = { "Dollars", "Pounds", "Dollars",
			"Deutsche Marks", "Francs", "Yen", "Guilders", "Lira",
			"Francs", "Dollars", "Dinars", "Pesos", "Dram", "Schillings",
			"Manat", "Dollars", "Dinar", "Taka", "Dollars", "Rouble",
			"Francs", "Dollars", "Boliviano", "Pula", "Real", "Lev",
			"Dollars", "Franc", "Pesos", "Yuan Renmimbi", "Dollars",
			"Pesos", "Kuna", "Pesos", "Pounds", "Koruna", "Kroner",
			"Pesos", "Dollars", "Sucre", "Pounds", "Colon", "Kroon",
			"Birr", "Pound", "Krone", "Dollars", "Markka", "Franc",
			"Pound", "Drachmas", "Dollars", "Dollars", "Forint", "Krona",
			"Rupees", "Rupiah", "Rial", "Dinar", "Punt", "Shekels",
			"Dollars", "Dinar", "Tenge", "Dinar", "Pounds", "Francs",
			"Ringgit", "Pesos", "Rupees", "Dollars", "Kroner", "Rupees",
			"Pesos", "Zloty", "Escudo", "Leu", "Rubles", "Riyal",
			"Dollars", "Koruna", "Rand", "Won", "Pesetas", "Dinar",
			"Krona", "Dollars", "Baht", "Dollars", "Lira", "Bolivar",
			"Kwacha" };

	private static final String[] SUBJECTS = { "ARTS", "BIOGRAPHIES", "BUSINESS", "CHILDREN",
			"COMPUTERS", "COOKING", "HEALTH", "HISTORY", "HOME", "HUMOR",
			"LITERATURE", "MYSTERY", "NON-FICTION", "PARENTING",
			"POLITICS", "REFERENCE", "RELIGION", "ROMANCE", "SELF-HELP",
			"SCIENCE-NATURE", "SCIENCE-FICTION", "SPORTS", "YOUTH",
			"TRAVEL" };

	private static final String[] BACKINGS = { "HARDBACK", "PAPERBACK", "USED", "AUDIO",
			"LIMITED-EDITION" };


	private static final String[] CREDITCARDS = { "VISA", "MASTERCARD", "DISCOVER", "AMEX",	"DINERS" };

	private static final String[] SHIPTYPES = { "AIR", "UPS", "FEDEX", "SHIP", "COURIER", "MAIL" };

	private static final String[] STATUSTYPES = { "PROCESSING", "SHIPPED", "PENDING", "DENIED" };


	public static String getRandomAString(int min, int max) {
		String newstring = new String();
		int i;
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
				'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
				'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
				'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%',
				'^', '&', '*', '(', ')', '_', '-', '=', '+', '{', '}', '[',
				']', /*'|',*/ ':', ';', '.', '?', '/', '~', ' ' }; // 77
		// characters
		int strlen = (int) Math.floor(rand.nextDouble() * ((max - min) + 1));
		strlen += min;
		for (i = 0; i < strlen; i++) {
			char c = chars[(int) Math.floor(rand.nextDouble() * 77)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}

	public static String getRandomAString(int length) {
		String newstring = new String();
		int i;
		final char[] chars = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
				'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u',
				'v', 'w', 'x', 'y', 'z', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
				'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '!', '@', '#', '$', '%',
				'^', '&', '*', '(', ')', '_', '-', '=', '+', '{', '}', '[',
				']', /*'|',*/ ':', ';', '.', '?', '/', '~', ' ' }; // 78*/
		// characters
		for (i = 0; i < length; i++) {
			char c = chars[(int) Math.floor(rand.nextDouble() * 77)];
			newstring = newstring.concat(String.valueOf(c));
		}
		return newstring;
	}

	public static int getRandomNString(int num_digits) {
		int return_num = 0;
		for (int i = 0; i < num_digits; i++) {
			return_num += getRandomInt(0, 9)
					* (int) java.lang.Math.pow(10.0, (double) i);
		}
		return return_num;
	}

	public static int getRandomNString(int min, int max) {
		int strlen = (int) Math.floor(rand.nextDouble() * ((max - min) + 1))
				+ min;
		return getRandomNString(strlen);
	}

	public static int getRandomInt(int lower, int upper) {
		int num = (int) Math.floor(rand.nextDouble() * ((upper + 1) - lower));
		if (num + lower > upper || num + lower < lower) {
			System.out.println("ERROR: Random returned value of of range!");
			System.exit(1);
		}
		return num + lower;
	}

	public static String DigSyl(int D, int N) {
		int i;
		String resultString = new String();
		String Dstr = Integer.toString(D);

		if (N > Dstr.length()) {
			int padding = N - Dstr.length();
			for (i = 0; i < padding; i++)
				resultString = resultString.concat("BA");
		}

		for (i = 0; i < Dstr.length(); i++) {
			if (Dstr.charAt(i) == '0')
				resultString = resultString.concat("BA");
			else if (Dstr.charAt(i) == '1')
				resultString = resultString.concat("OG");
			else if (Dstr.charAt(i) == '2')
				resultString = resultString.concat("AL");
			else if (Dstr.charAt(i) == '3')
				resultString = resultString.concat("RI");
			else if (Dstr.charAt(i) == '4')
				resultString = resultString.concat("RE");
			else if (Dstr.charAt(i) == '5')
				resultString = resultString.concat("SE");
			else if (Dstr.charAt(i) == '6')
				resultString = resultString.concat("AT");
			else if (Dstr.charAt(i) == '7')
				resultString = resultString.concat("UL");
			else if (Dstr.charAt(i) == '8')
				resultString = resultString.concat("IN");
			else if (Dstr.charAt(i) == '9')
				resultString = resultString.concat("NG");
		}

		return resultString;
	}

	public static long getRandomDate(int year, int month) {
		int day, maxday;

		maxday = 31;
		if (month == 3 | month == 5 | month == 8 | month == 10)
			maxday = 30;
		else if (month == 1)
			maxday = 28;

		day = getRandomInt(1, maxday);
		GregorianCalendar cal = new GregorianCalendar(year, month, day);

		return cal.getTime().getTime();
	}

	public static GregorianCalendar getCalendar() {
		return (GregorianCalendar) cal.clone();
	}

	public static String[] getCountries() {
		return COUNTRIES;
	}

	public static double[] getExchanges() {
		return EXCHANGES;
	}

	public static String[] getCurrencies() {
		return CURRENCIES;
	}

	public static String[] getSubjects() {
		return SUBJECTS;
	}

	public static String[] getBackings() {
		return BACKINGS;
	}

	public static String[] getCreditCards() {
		return CREDITCARDS;
	}

	public static String[] getShipTypes() {
		return SHIPTYPES;
	}

	public static String[] getStatusTypes() {
		return STATUSTYPES;
	}
}
