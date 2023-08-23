package main;

import java.time.Duration;
import java.time.LocalDateTime;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.operators.Projection;
import rq.common.operators.RecursiveUnrestricted;
import rq.common.operators.Restriction;
import rq.common.table.Attribute;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.table.TabularExpression;

public class Queries {

	public static TabularExpression electricityLoadDiagrams_repeatingPeaks(String cust, Double treshold, Duration period, Table iTable) {
		return RecursiveUnrestricted.factory(
				new Restriction(iTable, r -> r.getNoThrow("CUSTOMER") == cust && ((Double)r.getNoThrow("VALUE")) >= treshold),
				(Table t) -> 
					{
						try {
							return Projection.factory(
									null, 
									Schema.factory(
											new Attribute("CUSTOMER", String.class),
											new Attribute("TIME", LocalDateTime.class),
											new Attribute("VALUE", Double.class)))
							.eval();
						} catch (NotSubschemaException | DuplicateAttributeNameException e) {
							throw new RuntimeException(e);
						}
					});
	}
}
