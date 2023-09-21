package main;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Lukasiewitz;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnGreaterThan;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.Projection;
import rq.common.operators.RecursiveUnrestricted;
import rq.common.operators.Restriction;
import rq.common.similarities.NaiveSimilarity;
import rq.common.table.Attribute;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.files.io.LazyTable;

public class Queries {
	
	private static final Attribute customer = new Attribute("CUSTOMER", String.class);
	private static final Attribute time = new Attribute("TIME", LocalDateTime.class);
	private static final Attribute value = new Attribute("VALUE", Double.class);
	
	private static final Attribute rCustomer = new Attribute("right.CUSTOMER", String.class);
	private static final Attribute rTime = new Attribute("right.TIME", LocalDateTime.class);
	
	private static final Attribute aTime = new Attribute("addedTime", LocalDateTime.class);
	private static final Attribute raTime = new Attribute("right.addedTime", LocalDateTime.class);

	private static final Attribute rValue = new Attribute("right.VALUE", Double.class);
	
	/**
	 * Filters the table on customer and value threshold and adds a new attribute with period added to the time
	 * @param cust
	 * @param threshold
	 * @param period
	 * @param iTable
	 * @return
	 */
	public static TabularExpression electricityLoadDiagrams_CustTresholdAndPeriod(String cust, Double threshold, Duration period, MemoryTable iTable) {
		Schema intermediate;
		try {
			intermediate = Schema.factory(customer, time, value, aTime);
		} catch (DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}
		return new rq.common.operators.Map(
				new Restriction(iTable, r -> r.getNoThrow("CUSTOMER") == cust && ((Double)r.getNoThrow("VALUE")) >= threshold),
				r -> {
					try {
						return rq.common.table.Record.factory(
								intermediate, 
								Arrays.asList(
										new rq.common.table.Record.AttributeValuePair(customer, r.getNoThrow(customer)),
										new rq.common.table.Record.AttributeValuePair(time, r.getNoThrow(time)),
										new rq.common.table.Record.AttributeValuePair(value, r.getNoThrow(value)),
										new rq.common.table.Record.AttributeValuePair(aTime, ((LocalDateTime)r.getNoThrow(time)).plus(period))), 
								r.rank);
					} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
						throw new RuntimeException(e);
					}
				},
				s -> intermediate);
	}

	public static TabularExpression electricityLoadDiagrams_repeatingPeaks(MemoryTable iTable) {
		return RecursiveUnrestricted.factory(
				iTable,
				(Table t) -> 
					{
						try {
							return Projection.factory(
									Join.factory(
											t,
											iTable, 
											Lukasiewitz.PRODUCT, 
											Lukasiewitz.INFIMUM, 
											new OnEquals(customer, customer),
											new OnGreaterThan(time, time),
											new OnSimilar(aTime, time, NaiveSimilarity.DATETIME_SIMILARITY)),
									new Projection.To(rCustomer, customer),
									new Projection.To(rTime, time),
									new Projection.To(rValue, value),
									new Projection.To(raTime, aTime))
							.eval();
						} catch (DuplicateAttributeNameException | AttributeNotInSchemaException e) {
							throw new RuntimeException(e);
						}
					});
	}
	
	public static TabularExpression electricityLoadDiagrams_benchmark(Table iTable) 
			throws AttributeNotInSchemaException {
		Attribute cust = new Attribute("CUSTOMER", String.class);
		Attribute time = new Attribute("TIME", LocalDateTime.class);
		return Join.factory(
				iTable, 
				iTable, 
				Lukasiewitz.PRODUCT, 
				Lukasiewitz.INFIMUM, 
				new OnEquals(cust, cust),
				new OnEquals(time, time));
	}
	
	public static LazyExpression electricityLoadDiagrams_lazyBenchmark(LazyTable t1, LazyTable t2) {
		try {
			return LazyJoin.factory(
						t1,
						t2,
						Lukasiewitz.PRODUCT,
						Lukasiewitz.INFIMUM,
						new OnEquals(customer, customer),
						new OnEquals(time, time)
					);
		} catch (AttributeNotInSchemaException e) {
			//Unlikely
			throw new RuntimeException(e);
		}
	}
}
