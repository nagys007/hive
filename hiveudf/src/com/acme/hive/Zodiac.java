package com.acme.hive;

/*
 * 
$ hive
ADD JAR git/hiveudf/hiveudf/target/hiveudf-0.0.1-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION zodiac AS 'com.acme.hive.Zodiac';
DESCRIBE FUNCTION zodiac;
DESCRIBE FUNCTION EXTENDED zodiac; 
 SELECT zodiac(8,29) FROM dual;
 SELECT zodiac('1972-08-29') FROM dual;
 SELECT zodiac(to_date('1972-08-29 00:00:00')) FROM dual; -- processed as String
 SELECT zodiac(to_date(from_unixtime(unix_timestamp()))) FROM dual;
 SELECT zodiac(from_utc_timestamp('1972-08-29 00:00:00','CET')) FROM dual; -- processed as Date
 SELECT zodiac(from_unixtime(unix_timestamp())) FROM dual;
 */
import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.Description;

@Description(name = "zodiac", value = "_FUNC_ .. return the sign of the Zodiac", extended = "_FUNC_(date)\n"
		+ "_FUNC_(string) -> string format:yyyy-MM-dd\n"
		+ "_FUNC_(month,day)\n"
		+ "Examples:\n"
		+ "SELECT _FUNC_(to_date('1972-08-29')) FROM src;\n"
		+ "SELECT _FUNC_('1972-08-29') FROM src;\n"
		+ "SELECT _FUNC_(8,29) FROM src;\n")
public class Zodiac extends UDF {

	private SimpleDateFormat df;

	private final String Capricorn = "Capricorn";
	private final String Aquarius = "Aquarius";
	private final String Pisces = "Pisces";
	private final String Aries = "Aries";
	private final String Taurus = "Taurus";
	private final String Gemini = "Gemini";
	private final String Cancer = "Cancer";
	private final String Leo = "Leo";
	private final String Virgo = "Virgo";
	private final String Libra = "Libra";
	private final String Scorpio = "Scorpio";
	private final String Sagittarius = "Sagittarius";

	public Zodiac() {
		df = new SimpleDateFormat("yyyy-MM-dd");
	}

	public String evaluate(String bdays) {
		Date d = null;
		try {
			d = df.parse(bdays);
		} catch (Exception e) {
			return null;
		}
		return this.evaluate(d);
	}

	public String evaluate(Date bday) {
		//return bday.toString();
		Calendar c = Calendar.getInstance();
		c.setTime(bday);
		//return (c.get(Calendar.MONTH)+1) + " " + c.get(Calendar.DAY_OF_MONTH);*/
		return this.evaluate(c.get(Calendar.MONTH) + 1, c.get(Calendar.DAY_OF_MONTH));
	}

	public String evaluate(Integer month, Integer day) {
		switch (month) {
		case 1:
			if (day <= 20) {
				return Capricorn;
			} else {
				return Aquarius;
			}
		case 2:
			if (day <= 19) {
				return Aquarius;
			} else {
				return Pisces;
			}
		case 3:
			if (day <= 20) {
				return Pisces;
			} else {
				return Aries;
			}
		case 4:
			if (day <= 20) {
				return Aries;
			} else {
				return Taurus;
			}
		case 5:
			if (day <= 21) {
				return Taurus;
			} else {
				return Gemini;
			}
		case 6:
			if (day <= 21) {
				return Gemini;
			} else {
				return Cancer;
			}
		case 7:
			if (day <= 22) {
				return Cancer;
			} else {
				return Leo;
			}
		case 8:
			if (day <= 22) {
				return Leo;
			} else {
				return Virgo;
			}
		case 9:
			if (day <= 23) {
				return Virgo;
			} else {
				return Libra;
			}
		case 10:
			if (day <= 23) {
				return Libra;
			} else {
				return Scorpio;
			}
		case 11:
			if (day <= 22) {
				return Scorpio;
			} else {
				return Sagittarius;
			}
		case 12:
			if (day <= 21) {
				return Sagittarius;
			} else {
				return Capricorn;
			}
		default:
			return null;
		}

	}
}
