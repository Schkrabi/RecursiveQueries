package data;

import rq.common.table.Attribute;
import rq.common.types.DateTime;
import rq.common.types.Str10;
import rq.common.types.Str50;

public class Retail {
	public static final Attribute<Str10> invoice = new Attribute<>("Invoice", Str10.class);
	public static final Attribute<Str10> stockCode = new Attribute<>("StockCode", Str10.class);
	public static final Attribute<Str50> description = new Attribute<>("Description", Str50.class);
	public static final Attribute<Double> quantity = new Attribute<>("Quantity", Double.class);
	public static final Attribute<DateTime> invoiceDate = new Attribute<>("InvoiceDate", DateTime.class);
	public static final Attribute<Double> price = new Attribute<>("Price", Double.class);
	public static final Attribute<Str10> customerId = new Attribute<>("Customer ID", Str10.class);
	public static final Attribute<Str50> coutnry = new Attribute<>("Country", Str50.class);
	
	public static final Attribute<Double> priceMovAvg = new Attribute<>("PRICE_MAVG", Double.class);
	public static final Attribute<Double> qtyMovAvg = new Attribute<>("QTY_MAVG", Double.class);
	public static final Attribute<Integer> peaks = new Attribute<>("PEAKS", Integer.class);
	public static final Attribute<DateTime> fromTime = new Attribute<>("FROM_TIME", DateTime.class);
	public static final Attribute<DateTime> toTime = new Attribute<>("TO_TIME", DateTime.class);
}
