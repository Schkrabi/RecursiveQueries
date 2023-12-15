package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import data.Electricity;
import queries.Queries.Algorithm;
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
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
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
import rq.common.tools.Counter;

@CallingArg("electricity_noCust_unrtra")
public class Queries_Electricity_NoCust_UnrTra extends Queries {
	
	private final Duration TIME_STEP = Duration.ofDays(365);
	private final Duration SIMILARITY_SCALE = Duration.ofDays(30);
	private Double PEAK_MULTIPLIER = 1.45d;
	private Double PEAK_SIMILARITY = 0.2d; // I am still somewhat interested in peaks of 1.3 of moving avg
	private final int K = 1000;
	private final int SEARCHED_NUMBER_OF_PEAKS = 3;
	
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

	public Queries_Electricity_NoCust_UnrTra(Algorithm algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}
	public static Queries factory(Algorithm algorithm, AlgorithmMonitor monitor) {
		return new Queries_Electricity_NoCust_UnrTra(algorithm, monitor);
	}
	
	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("electricity")
				.append("noCust")
				.append("unrXTra")
				.append("_timeStep=").append(TIME_STEP.toString())
				.append("_similarityScale=").append(SIMILARITY_SCALE.toString())
				.append("_peakMultiplier").append(PEAK_MULTIPLIER)
				.append("_peakSimilarity").append(PEAK_SIMILARITY)
				.append("_K=").append(K)
				.append("numberOfPeaks=").append(SEARCHED_NUMBER_OF_PEAKS)
				.toString();
	}

	public LazyExpression caseNoCustPrepare(LazyExpression iTable) {
		LazyExpression le =
				LazyRestriction.factory(
						iTable, 
						peakRanker);
		
		return le;
	}
	
	@Override
	protected LazyExpression prepareUnrestricted(LazyExpression iTable) {
		return this.caseNoCustPrepare(iTable);
	}

	@Override
	protected LazyExpression prepareTopK(LazyExpression iTable) {
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		return this.caseNoCustPrepare(iTable);
	}

	@Override
	protected TabularExpression queryUnterstricted(Table iTable) {
		TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveUnrestricted.factory(
					LazyProjection.factory(
							new LazyFacade(iTable),
							new Projection.To(Electricity.time, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new Constant<Integer>(1), Electricity.peaks)), 
					(Table t) -> {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											new LazyFacade(iTable), 
											new OnSimilar(
													new PlusDateTime(Electricity.toTime, TIME_STEP), 
													Electricity.time, 
													LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
									new Projection.To(Electricity.fromTime, Electricity.fromTime),
									new Projection.To(Electricity.time, Electricity.toTime),
									new Projection.To(new PlusInteger(Electricity.peaks, new Constant<Integer>(1)), Electricity.peaks));
						} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException
								| OnOperatornNotApplicableToSchemaException e) {
							throw new RuntimeException(e);
						}
					},
					this.monitor);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
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
		TabularExpression exp;
		try {
			exp = LazyRecursiveTransformed.factory(
					LazyProjection.factory(
							new LazyFacade(iTable),  
							new Projection.To(Electricity.time, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new Constant<Integer>(1), Electricity.peaks)), 
					(Table t) -> {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											new LazyFacade(iTable), 
											new OnSimilar(
													new PlusDateTime(Electricity.toTime, TIME_STEP), 
													Electricity.time, 
													LinearSimilarity.dateTimeSimilarityUntil(SIMILARITY_SCALE.toSeconds()))), 
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
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
			throw new RuntimeException(e);
		}
		
		return exp;
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		return new Restriction(
				iTable,
				r -> {
					int numOfPeaks = (Integer) r.getNoThrow(Electricity.peaks);
					if (numOfPeaks >= SEARCHED_NUMBER_OF_PEAKS) {
						return r.rank;
					}
					if (numOfPeaks == 1) {
						return 0.0d;
					}
					return Goguen.PRODUCT.apply(r.rank, (double) numOfPeaks / (double) SEARCHED_NUMBER_OF_PEAKS);
				},
				(s, k) -> TopKTable.factory(s, K));
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
