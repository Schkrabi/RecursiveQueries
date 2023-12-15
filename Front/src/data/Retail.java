package data;

import rq.common.table.Attribute;
import rq.common.types.DateTime;
import rq.common.types.Str10;
import rq.common.types.Str50;

public class Retail {
	public static final Attribute invoice = new Attribute("Invoice", Str10.class);
	public static final Attribute stockCode = new Attribute("StockCode", Str10.class);
	public static final Attribute description = new Attribute("Description", Str50.class);
	public static final Attribute quantity = new Attribute("Quantity", Integer.class);
	public static final Attribute invoiceDate = new Attribute("InvoiceDate", DateTime.class);
	public static final Attribute price = new Attribute("Price", Double.class);
	public static final Attribute customerId = new Attribute("Customer ID", Str10.class);
	public static final Attribute coutnry = new Attribute("Country", Str50.class);
	
	public static final Attribute priceMovAvg = new Attribute("PRICE_MAVG", Double.class);
	public static final Attribute qtyMovAvg = new Attribute("QTY_MAVG", Double.class);
	public static final Attribute peaks = new Attribute("PEAKS", Integer.class);
	public static final Attribute fromTime = new Attribute("FROM_TIME", DateTime.class);
	public static final Attribute toTime = new Attribute("TO_TIME", DateTime.class);
}
