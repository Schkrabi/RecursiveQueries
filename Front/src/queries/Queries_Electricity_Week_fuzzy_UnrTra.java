package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import data.Electricity;
import rq.common.algorithms.LazyRecursiveTransformed;
import rq.common.algorithms.LazyRecursiveUnrestricted;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Goguen;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.table.TopKTable;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("electricity_week_fuzzy_unrtra")
public class Queries_Electricity_Week_fuzzy_UnrTra extends Queries {
	
	public Queries_Electricity_Week_fuzzy_UnrTra(Algorithm algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}
	public static Queries factory(Algorithm algorithm, AlgorithmMonitor monitor) {
		return new Queries_Electricity_Week_fuzzy_UnrTra(algorithm, monitor);
	}

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

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("electricity")
				.append("Week")
				.append("fuzzy")
				.append("unrXtra")
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
		throw this.algorithmNotSupportedException();
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
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression queryTransformed(Table iTable) {
		TabularExpression exp = null;
		
		try {
		exp = LazyRecursiveTransformed.factory(
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
				r -> LazyRestriction.factory(
							new LazyFacade(MemoryTable.of(r)),
							re -> {
								int numOfPeaks = (Integer)re.getNoThrow(Electricity.peaks);
								if(numOfPeaks >= SEARCHED_NUMBER_OF_PEAKS) {
									return r.rank;
								}
								if(numOfPeaks == 1) {
									return 0.0d;
								}
								return Goguen.PRODUCT.apply(r.rank, (double)numOfPeaks / (double)SEARCHED_NUMBER_OF_PEAKS);
							}),
				this.monitor);
		
		}catch(RecordValueNotApplicableOnSchemaException | DuplicateAttributeNameException e) {
			throw new RuntimeException(e);
		}
		return exp;
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		TabularExpression exp = null;
		
		exp = new Restriction(
				iTable,
				r -> {
					int numOfPeaks = (Integer)r.getNoThrow(Electricity.peaks);
					if(numOfPeaks >= SEARCHED_NUMBER_OF_PEAKS) {
						return r.rank;
					}
					if(numOfPeaks == 1) {
						return 0.0d;
					}
					return Goguen.PRODUCT.apply(r.rank, (double)numOfPeaks / (double)SEARCHED_NUMBER_OF_PEAKS);
				},
				(s, k) -> TopKTable.factory(s, K));
		
		return exp;
	}

	@Override
	protected TabularExpression postprocessTopK(Table iTable) {
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression postprocessTransformed(Table iTable) {
		return iTable;
	}

}
