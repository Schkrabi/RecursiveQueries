package data;

import rq.common.table.Attribute;
import rq.common.types.DateTime;
import rq.common.types.Str10;

public class Electricity {

	// File columns
	public final static Attribute customer = new Attribute("CUSTOMER", Str10.class);
	public final static Attribute time = new Attribute("TIME", DateTime.class);
	public final static Attribute value = new Attribute("VALUE", Double.class);
	public final static Attribute movingAvg = new Attribute("MOVING_AVG", Double.class);
	public final static Attribute fromTime = new Attribute("FROM_TIME", DateTime.class);
	public final static Attribute toTime = new Attribute("TO_TIME", DateTime.class);
	public final static Attribute peaks = new Attribute("PEAKS", Integer.class);
	// Computed columns
	public final static Attribute aTime = new Attribute("addedTime", DateTime.class);

}
