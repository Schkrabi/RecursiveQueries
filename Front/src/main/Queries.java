package main;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotComparableException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Lukasiewitz;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnGreaterThan;
import rq.common.onOperators.OnLesserThan;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.RecursiveUnrestricted;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.similarities.NaiveSimilarity;
import rq.common.table.Attribute;
import rq.common.table.FileMappedTable;
import rq.common.table.LazyFacade;
import rq.common.table.Schema;
import rq.common.types.Str10;
import rq.common.table.MemoryTable;
import rq.files.io.LazyTable;
import rq.common.types.DateTime;

public class Queries {
	
	private static final Attribute customer = new Attribute("CUSTOMER", Str10.class);
	private static final Attribute time = new Attribute("TIME", DateTime.class);
	private static final Attribute value = new Attribute("VALUE", Double.class);
	
	private static final Attribute rCustomer = new Attribute("right.CUSTOMER", Str10.class);
	private static final Attribute rTime = new Attribute("right.TIME", DateTime.class);
	
	private static final Attribute aTime = new Attribute("addedTime", DateTime.class);
	private static final Attribute raTime = new Attribute("right.addedTime", DateTime.class);

	private static final Attribute rValue = new Attribute("right.VALUE", Double.class);
	
	public static LazyExpression electricityLoadDiagrams_CustTresholdAndPeriodLazy(Str10 cust, Double threshold, Duration period, LazyTable iTable) {
		Schema intermediate;
		try {
			intermediate = Schema.factory(customer, time, value, aTime);
		} catch (DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}
		
		return rq.common.operators.LazyMapping.factory(
				LazyRestriction.factory(
						iTable, 
						r -> /*r.getNoThrow("CUSTOMER").equals(cust) 
						&&*/ ((Double)r.getNoThrow("VALUE")) >= threshold ? 1.0d : 0.0d), 
				r -> {
					try {
						return rq.common.table.Record.factory(
								intermediate, 
								Arrays.asList(
										new rq.common.table.Record.AttributeValuePair(customer, r.getNoThrow(customer)),
										new rq.common.table.Record.AttributeValuePair(time, r.getNoThrow(time)),
										new rq.common.table.Record.AttributeValuePair(value, r.getNoThrow(value)),
										new rq.common.table.Record.AttributeValuePair(aTime, new DateTime(((DateTime)r.getNoThrow(time)).getInner().plus(period)))), 
								r.rank);
					} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				s -> intermediate);
	}
	
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
										new rq.common.table.Record.AttributeValuePair(aTime, new DateTime(((DateTime)r.getNoThrow(time)).getInner().plus(period)))),
								r.rank);
					} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
						throw new RuntimeException(e);
					}
				},
				s -> intermediate);
	}

	public static TabularExpression electricityLoadDiagrams_repeatingPeaks(Table iTable) {
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
											new OnSimilar(aTime, time, NaiveSimilarity.LOCALDATETIME_SIMILARITY_MINUTES)),
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
	
	public static TabularExpression electricityLoadDiagrams_repeatingPeaksMapped(Table iTable) {
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
											new OnSimilar(aTime, time, NaiveSimilarity.LOCALDATETIME_SIMILARITY_MINUTES)),
									new Projection.To(rCustomer, customer),
									new Projection.To(rTime, time),
									new Projection.To(rValue, value),
									new Projection.To(raTime, aTime))
							.eval();
						} catch (DuplicateAttributeNameException | AttributeNotInSchemaException e) {
							throw new RuntimeException(e);
						}
					},
				FileMappedTable.supplier);
	}
	
	public static TabularExpression electricityLoadDiagrams_repeatingPeaks_Mapped_Lazy(Table iTable, LocalDateTime initialUntil) {
		return LazyRecursiveUnrestricted.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable),
						r -> ((DateTime)(r.getNoThrow(time))).getInner().compareTo(initialUntil) <= 0 ? 1.0d : 0.0d), 
				(Table table) -> {
					try {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(table), 
											new LazyFacade(iTable), 
											Lukasiewitz.PRODUCT, 
											Lukasiewitz.INFIMUM, 
											new OnEquals(customer, customer),
											OnLesserThan.factory(time, time),
											new OnSimilar(aTime, time, LinearSimilarity.dateTimeSimilarityUntil(3600))), 
									new Projection.To(rCustomer, customer),
									new Projection.To(rTime, time),
									new Projection.To(rValue, value),
									new Projection.To(raTime, aTime));
						} catch (NotComparableException | ComparableDomainMismatchException e) {
							// Unlikely
							throw new RuntimeException(e);
						}
					} catch (AttributeNotInSchemaException | DuplicateAttributeNameException e) {
						// Unlikely
						throw new RuntimeException(e);
					}
				},
				(Schema s) -> {
					try {
						return FileMappedTable.factory(s, 1_000_000);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				},
				(Schema s) -> {
					try {
						return FileMappedTable.factory(s, 100_000);
					} catch (IOException e) {
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
