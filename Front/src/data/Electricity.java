package data;

import rq.common.table.Attribute;
import rq.common.types.DateTime;
import rq.common.types.Str10;

public class Electricity {

	// File columns
	public final static Attribute<Str10> customer = new Attribute<>("CUSTOMER", Str10.class);
	public final static Attribute<DateTime> time = new Attribute<>("TIME", DateTime.class);
	public final static Attribute<Double> value = new Attribute<>("VALUE", Double.class);
	public final static Attribute<Double> movingAvg = new Attribute<>("MOVING_AVG", Double.class);
	public final static Attribute<DateTime> fromTime = new Attribute<>("FROM_TIME", DateTime.class);
	public final static Attribute<DateTime> toTime = new Attribute<>("TO_TIME", DateTime.class);
	public final static Attribute<Integer> peaks = new Attribute<>("PEAKS", Integer.class);
	// Computed columns
	public final static Attribute<DateTime> aTime = new Attribute<>("addedTime", DateTime.class);

}
