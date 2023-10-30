package main;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyMapping;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveTopK;
import rq.common.operators.LazyRecursiveTransformed;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.table.Schema;
import rq.common.types.DateTime;
import rq.common.types.Str10;

import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.OnLesserThan;

public class Queries_Electricity {
	private final Duration CASE_WEEK_TIME_STEP = Duration.ofDays(365);
	private final Duration CASE_WEEK_SIMILARITY_SCALE = Duration.ofDays(60);
	private final LocalDateTime CASE_WEEK_INITIAL = LocalDateTime.of(2011, 12, 31, 0, 0, 0);
	private final Double CASE_WEEK_PEAK_MULTIPLIER = 1.1d;
	private final Str10 CASE_WEEK_CUSTOMER = Str10.factory("MT_124");
	private final int CASE_WEEK_K = 1000;
	
	private final Duration CASE_NOCUST_TIME_STEP = Duration.ofHours(24);
	private final Duration CASE_NOCUST_SIMILARITY_SCALE = Duration.ofHours(6);
	private final LocalDateTime CASE_NOCUST_INITIAL = LocalDateTime.of(2011,  1, 8, 0, 0, 0);
	private Double CASE_NOCUST_PEAK_MULTIPLIER = 1.15d;
	private final LocalDateTime CASE_NOCUST_AFTER = LocalDateTime.of(2014, 1, 1, 0, 0, 0);
	private final int CASE_NOCUST_K = 100;
	
	// File columns
	private final Attribute customer = new Attribute("CUSTOMER", Str10.class);
	private final Attribute time = new Attribute("TIME", DateTime.class);
	private final Attribute value = new Attribute("VALUE", Double.class);
	private final Attribute movingAvg = new Attribute("MOVING_AVG", Double.class);

	// Computed columns
	private final Attribute aTime = new Attribute("addedTime", DateTime.class);

	// Schemas
	private final Schema caseWeekPrepared;
	private final Schema caseNoCustPrepared;

	// Auxiliary joinc columns
	private final Attribute rCustomer = new Attribute("right.CUSTOMER", Str10.class);
	private final Attribute rTime = new Attribute("right.TIME", DateTime.class);
	private final Attribute raTime = new Attribute("right.addedTime", DateTime.class);
	private final Attribute rValue = new Attribute("right.VALUE", Double.class);
	private final Attribute rMovingAvg = new Attribute("right.MOVING_AVG", Double.class);

	private Queries_Electricity() {
		try {
			caseWeekPrepared = Schema.factory(customer, time, value, aTime, movingAvg);
			caseNoCustPrepared = Schema.factory(time, value, aTime, movingAvg);
		} catch (DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}
	}

	// File electricity2_agg168.csv
	public LazyExpression caseWeekPrepare(LazyExpression iTable) {
		LazyExpression le = 
			LazyMapping.factory(
				LazyRestriction.factory(
						iTable,
						r -> (double) r.getNoThrow(value) > CASE_WEEK_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d), 
				r -> {
					try {
						return rq.common.table.Record.factory(
								caseWeekPrepared,
								Arrays.asList(
										new rq.common.table.Record.AttributeValuePair(customer, r.getNoThrow(customer)),
										new rq.common.table.Record.AttributeValuePair(time, r.getNoThrow(time)),
										new rq.common.table.Record.AttributeValuePair(value, r.getNoThrow(value)),
										new rq.common.table.Record.AttributeValuePair(movingAvg, r.getNoThrow(movingAvg)),
										new rq.common.table.Record.AttributeValuePair(aTime, new DateTime(
												((DateTime) r.getNoThrow(time)).getInner().plus(CASE_WEEK_TIME_STEP)))),
								r.rank);
					} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
						throw new RuntimeException(e);
					}
				});

		return le;
	}

	public TabularExpression caseWeekUnrestricted(Table iTable) {

		TabularExpression exp = LazyRecursiveUnrestricted.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) <= -1 ? r.rank : 0.0d), 
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
														r -> (double) r.getNoThrow(value) > CASE_WEEK_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
												//		r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) < 1 ? r.rank : 0.0d),
												new OnEquals(customer, customer), 
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_WEEK_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rCustomer, customer), 
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				});

		return exp;
	}
	
	public TabularExpression caseWeekTopK(Table iTable) {
		TabularExpression exp = LazyRecursiveTopK.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) <= -1 ? r.rank : 0.0d),
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
														r -> (double) r.getNoThrow(value) > CASE_WEEK_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
												//		r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) < 1 ? r.rank : 0.0d),
												new OnEquals(customer, customer), 
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_WEEK_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rCustomer, customer), 
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				CASE_WEEK_K);
		
		return exp;
	}
	
	public TabularExpression caseWeekTransformed(Table iTable) {
		TabularExpression exp = LazyRecursiveTransformed.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) <= -1 ? r.rank : 0.0d), 
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
														r -> (double) r.getNoThrow(value) > CASE_WEEK_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
												//		r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_WEEK_INITIAL) < 1 ? r.rank : 0.0d),
												new OnEquals(customer, customer), 
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_WEEK_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rCustomer, customer), 
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				CASE_WEEK_K, 
				r -> LazyRestriction.factory(new LazyFacade(MemoryTable.of(r)), re -> r.getNoThrow(customer).equals(CASE_WEEK_CUSTOMER) ? re.rank : 0.0d));
		return exp;
	}
	
	//Case No Custmer
	public LazyExpression caseNoCustPrepare(LazyExpression iTable) {
		LazyExpression le =
				LazyMapping.factory(
						LazyRestriction.factory(
								iTable,
								r -> (double) r.getNoThrow(value) > CASE_NOCUST_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d), 
						r -> {
							try {
								return rq.common.table.Record.factory(caseNoCustPrepared,
										Arrays.asList(
												new rq.common.table.Record.AttributeValuePair(time, r.getNoThrow(time)),
												new rq.common.table.Record.AttributeValuePair(value, r.getNoThrow(value)),
												new rq.common.table.Record.AttributeValuePair(movingAvg, r.getNoThrow(movingAvg)),
												new rq.common.table.Record.AttributeValuePair(aTime, new DateTime(
														((DateTime) r.getNoThrow(time)).getInner().plus(CASE_NOCUST_TIME_STEP)))),
										r.rank);
							} catch (TypeSchemaMismatchException | AttributeNotInSchemaException e) {
								throw new RuntimeException(e);
							}
						});
		
		return le;
	}
	
	public TabularExpression caseNoCustUnrestricted(Table iTable) {
		TabularExpression exp = LazyRecursiveUnrestricted.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) <= -1 ? r.rank : 0.0d), 
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
												//		r -> (double) r.getNoThrow(value) > CASE_NOCUST_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
														r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) > -1 ? r.rank : 0.0d),
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_NOCUST_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				});

		return exp;
	}
	
	public TabularExpression caseNoCustTopK(Table iTable) {
		TabularExpression exp = LazyRecursiveTopK.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) <= -1 ? r.rank : 0.0d), 
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
												//		r -> (double) r.getNoThrow(value) > CASE_NOCUST_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
														r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) > -1 ? r.rank : 0.0d),
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_NOCUST_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				CASE_NOCUST_K);
		return exp;
	}
	
	public TabularExpression caseNoCustTransformed(Table iTable) {
		TabularExpression exp = LazyRecursiveTransformed.factory(
				LazyRestriction.factory(
						new LazyFacade(iTable), 
						r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) <= -1 ? r.rank : 0.0d), 
				(Table t) -> {
					try {
						return LazyProjection
								.factory(
										LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
												//		r -> (double) r.getNoThrow(value) > CASE_NOCUST_PEAK_MULTIPLIER * (double) r.getNoThrow(movingAvg) ? r.rank : 0.0d),
														r -> ((DateTime) r.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_INITIAL) > -1 ? r.rank : 0.0d),
												new OnLesserThan(time, time),
												new OnSimilar(aTime, time,
														LinearSimilarity
																.dateTimeSimilarityUntil(CASE_NOCUST_SIMILARITY_SCALE.toSeconds()))),
										new Projection.To(rTime, time),
										new Projection.To(rValue, value), 
										new Projection.To(raTime, aTime),
										new Projection.To(rMovingAvg, movingAvg));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				},
				CASE_NOCUST_K, 
				r -> LazyRestriction.factory(
								new LazyFacade(MemoryTable.of(r)), 
								re -> ((DateTime)re.getNoThrow(time)).getInner().compareTo(CASE_NOCUST_AFTER) == 1 ? re.rank : 0.0d)
				);
		
		return exp;
	}
	
	private static Queries_Electricity singleton = null;
	public static Queries_Electricity instance() {
		if(singleton == null) {
			singleton = new Queries_Electricity();
		}
		return singleton;
	}
}
