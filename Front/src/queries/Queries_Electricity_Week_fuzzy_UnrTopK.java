package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import data.Electricity;
import rq.common.algorithms.LazyRecursiveTopK;
import rq.common.algorithms.LazyRecursiveUnrestricted;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Goguen;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.TopKTable;
import rq.common.tools.AlgorithmMonitor;
import rq.common.tools.Counter;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.onOperators.OnLesserThan;

@CallingArg("electricity_week_fuzzy_unrtopk")
public class Queries_Electricity_Week_fuzzy_UnrTopK extends Queries {
	private final Duration TIME_STEP = Duration.ofDays(60);
	private final Duration SIMILARITY_SCALE = Duration.ofDays(20);
	private final Double PEAK_MULTIPLIER = 1.2d;
	private Double PEAK_SIMILARITY = 0.2d;
	private final int K = 8000;
	private final int SEARCHED_NUMBER_OF_PEAKS = 10;
	
	private final BiFunction<Object, Object, Double> peakSimilarity = 
			LinearSimilarity.doubleSimilarityUntil(PEAK_SIMILARITY);
	private final Function<rq.common.table.Record, Double> peakRanker =
			(rq.common.table.Record r) -> {
				double value = (double)r.getNoThrow(Electricity.value);
				double moving_avg = (double)r.getNoThrow(Electricity.movingAvg);
				double div = value/moving_avg;
				if(div >= PEAK_MULTIPLIER) {
					return r.rank;
				}
				return Goguen.PRODUCT.apply(r.rank, peakSimilarity.apply(PEAK_SIMILARITY, div));
			};

	public Queries_Electricity_Week_fuzzy_UnrTopK(Algorithm algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}
	public static Queries factory(Algorithm algorithm, AlgorithmMonitor monitor) {
		return new Queries_Electricity_Week_fuzzy_UnrTopK(algorithm, monitor);
	}
	
	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("electricity")
				.append("Week")
				.append("fuzzy")
				.append("unrXtopK")
				.append("_timeStep=").append(TIME_STEP.toString())
				.append("_similarityScale=").append(SIMILARITY_SCALE.toString())
				.append("_peakMultiplier=").append(PEAK_MULTIPLIER)
				.append("_peakSimilarity=").append(PEAK_SIMILARITY)
				.append("_K=").append(K)
				.append("_numberOfPeaks=").append(SEARCHED_NUMBER_OF_PEAKS)
				.toString();
	}

	// File electricity2_agg168.csv
	public LazyExpression caseWeekPrepare(LazyExpression iTable) {
		LazyExpression le = 
				LazyRestriction.factory(
						iTable, 
						peakRanker);

		return le;
	}

	@Override
	protected LazyExpression prepareUnrestricted(LazyExpression iTable) {
		return this.caseWeekPrepare(iTable);
	}

	@Override
	protected LazyExpression prepareTopK(LazyExpression iTable) {
		return this.caseWeekPrepare(iTable);
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		return this.caseWeekPrepare(iTable);
	}

	@Override
	protected TabularExpression queryUnterstricted(Table iTable) {
		TabularExpression exp = null;
		
		try {
		exp = LazyRecursiveUnrestricted.factory(				
				LazyProjection.factory(
						new LazyFacade(iTable), 
						new Projection.To(Electricity.customer, Electricity.customer),
						new Projection.To(Electricity.time, Electricity.fromTime),
						new Projection.To(Electricity.time, Electricity.toTime),
						new Projection.To(new Constant<Integer>(1), Electricity.peaks)),
				(Table t) -> 
					{
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											new LazyFacade(iTable),  
											new OnEquals(Electricity.customer, Electricity.customer),
											new OnLesserThan(Electricity.toTime, Electricity.time),
											new OnSimilar(
													new PlusDateTime(Electricity.toTime, TIME_STEP), 
													Electricity.time, 
													LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
									new Projection.To(Join.left(Electricity.customer), Electricity.customer),
									new Projection.To(Electricity.fromTime, Electricity.fromTime),
									new Projection.To(Electricity.time, Electricity.toTime),
									new Projection.To(new PlusInteger(Electricity.peaks, new Constant<Integer>(1)), Electricity.peaks));
						} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException
								| OnOperatornNotApplicableToSchemaException e) {
							throw new RuntimeException(e);
						}
					},
					this.monitor);
		}catch(RecordValueNotApplicableOnSchemaException | DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}

		return exp;
	}

	@Override
	protected TabularExpression queryTopK(Table iTable) {
		TabularExpression exp = null;
		
		try {
		exp = LazyRecursiveTopK.factory(
				LazyProjection.factory(
						new LazyFacade(iTable),  
						new Projection.To(Electricity.customer, Electricity.customer),
						new Projection.To(Electricity.time, Electricity.fromTime),
						new Projection.To(Electricity.time, Electricity.toTime),
						new Projection.To(new Constant<Integer>(1), Electricity.peaks)),
				(Table t) -> 
				{
					try {
						return LazyProjection.factory(
								LazyJoin.factory(
										new LazyFacade(t), 
										new LazyFacade(iTable),  
										new OnEquals(Electricity.customer, Electricity.customer),
										new OnLesserThan(Electricity.toTime, Electricity.time),
										new OnSimilar(
												new PlusDateTime(Electricity.toTime, TIME_STEP), 
												Electricity.time, 
												LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
								new Projection.To(Join.left(Electricity.customer), Electricity.customer),
								new Projection.To(Electricity.fromTime, Electricity.fromTime),
								new Projection.To(Electricity.time, Electricity.toTime),
								new Projection.To(new PlusInteger(Electricity.peaks, new Constant<Integer>(1)), Electricity.peaks));
					} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException
							| OnOperatornNotApplicableToSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				K,
				this.monitor);
		}catch(RecordValueNotApplicableOnSchemaException | DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}
		
		return exp;
	}

	@Override
	protected TabularExpression queryTransformed(Table iTable) {
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		TabularExpression exp = null;
		
		exp = new Restriction(
				iTable,
				r -> r.rank,
				(s, k) -> TopKTable.factory(s, K));
		
		return exp;
	}

	@Override
	protected TabularExpression postprocessTopK(Table iTable) {
		return iTable;
	}

	@Override
	protected TabularExpression postprocessTransformed(Table iTable) {
		return iTable;
	}
}
