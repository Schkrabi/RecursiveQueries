package queries;

import java.time.Duration;
import java.util.function.BiFunction;
import java.util.function.Function;

import data.Electricity;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Goguen;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnLesserThan;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.PlusDateTime;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveTopK;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.TopKTable;
import rq.common.tools.Counter;

@CallingArg("electricity_noCust_unrtopk")
public class Queries_Electricity_NoCust_UnrTopK extends Queries {
	
	//Trying to find large peaks (~over 1.45 moving average) with period of a year
	
	private final Duration TIME_STEP = Duration.ofDays(365);
	private final Duration SIMILARITY_SCALE = Duration.ofDays(30);
	private Double PEAK_MULTIPLIER = 1.45d;
	private Double PEAK_SIMILARITY = 0.2d; // I am still somewhat interested in peaks of 1.3 of moving avg
	private final int K = 1000;
	
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
			
	public Queries_Electricity_NoCust_UnrTopK(Algorithm algorithm, Counter counter) {
		super(algorithm, counter);
	}
	public static Queries factory(Algorithm algorithm, Counter counter) {
		return new Queries_Electricity_NoCust_UnrTopK(algorithm, counter);
	}
	
	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("electricity")
				.append("NoCust")
				.append("unrXTopK")
				.append("_timeStep=").append(TIME_STEP.toString())
				.append("_similarityScale=").append(SIMILARITY_SCALE.toString())
				.append("_peakMultiplier=").append(PEAK_MULTIPLIER)
				.append("_peakSimilarity=").append(PEAK_SIMILARITY)
				.append("_K=").append(K)
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
		return this.caseNoCustPrepare(iTable);
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		throw this.algorithmNotSupportedException();
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
											new OnLesserThan(Electricity.toTime, Electricity.time),
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
					recordCounter);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
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
							new Projection.To(Electricity.time, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new Constant<Integer>(1), Electricity.peaks)), 
					(Table t) -> {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											new LazyFacade(iTable),
											new OnLesserThan(Electricity.toTime, Electricity.time),
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
					recordCounter);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
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
		throw this.algorithmNotSupportedException();
	}

}
