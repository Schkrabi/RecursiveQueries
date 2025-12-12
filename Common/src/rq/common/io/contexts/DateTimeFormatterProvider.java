package rq.common.io.contexts;

import java.time.format.DateTimeFormatter;

public class DateTimeFormatterProvider {

	private DateTimeFormatter  formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
//	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy H:mm");
	
	private DateTimeFormatterProvider() {
	}
	
	public DateTimeFormatter get() {
		return this.formatter;
	}
	
	private static DateTimeFormatterProvider singleton = new DateTimeFormatterProvider();
	
	public static DateTimeFormatterProvider instance() {
		if(DateTimeFormatterProvider.singleton == null) {
			DateTimeFormatterProvider.singleton = new DateTimeFormatterProvider();
		}
		return DateTimeFormatterProvider.singleton;
	}
	
	public static DateTimeFormatter formatter() {
		return instance().get();
	}
}
