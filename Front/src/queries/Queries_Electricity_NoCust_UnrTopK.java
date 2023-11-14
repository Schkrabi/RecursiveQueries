package queries;

import java.time.Duration;

import data.Electricity;
import queries.Queries.Algorithm;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.Constant;
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
	
	private final Duration TIME_STEP = Duration.ofHours(24);
	private final Duration SIMILARITY_SCALE = Duration.ofHours(6);
	private Double PEAK_MULTIPLIER = 1.3d;
	private Double PEAK_MULTIPLIER_AFTER_FIRST = 1.15d;
	private final int K = 100;
	
	public Queries_Electricity_NoCust_UnrTopK(Algorithm algorithm, Counter counter) {
		super(algorithm, counter);
	}
	public static Queries factory(Algorithm algorithm, Counter counter) {
		return new Queries_Electricity_NoCust_UnrTopK(algorithm, counter);
	}
	
	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Electricity")
				.append("NoCust")
				.append("unrXTopK")
				.append("_timeStep=").append(TIME_STEP.toString())
				.append("_similarityScale=").append(SIMILARITY_SCALE.toString())
				.append("_peakMultiplier=").append(PEAK_MULTIPLIER)
				.append("_peakMultiplierAfterFirst=").append(PEAK_MULTIPLIER_AFTER_FIRST)
				.append("_K=").append(K)
				.toString();
	}
	
	public LazyExpression caseNoCustPrepare(LazyExpression iTable) {
		LazyExpression le =
				LazyRestriction.factory(
						iTable, 
						r -> (Double)r.getNoThrow(Electricity.value) > (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d);
		
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
							LazyRestriction.factory(
									new LazyFacade(iTable), 
									r -> (Double)r.getNoThrow(Electricity.value) > PEAK_MULTIPLIER * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
							new Projection.To(Electricity.time, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new Constant<Integer>(1), Electricity.peaks)), 
					(Table t) -> {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											LazyRestriction.factory(
													new LazyFacade(iTable), 
													r -> (Double)r.getNoThrow(Electricity.value) > PEAK_MULTIPLIER_AFTER_FIRST * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
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
							LazyRestriction.factory(
									new LazyFacade(iTable), 
									r -> (Double)r.getNoThrow(Electricity.value) > PEAK_MULTIPLIER * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
							new Projection.To(Electricity.time, Electricity.fromTime),
							new Projection.To(Electricity.time, Electricity.toTime),
							new Projection.To(new Constant<Integer>(1), Electricity.peaks)), 
					(Table t) -> {
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(t), 
											LazyRestriction.factory(
													new LazyFacade(iTable), 
													r -> (Double)r.getNoThrow(Electricity.value) > PEAK_MULTIPLIER_AFTER_FIRST * (Double)r.getNoThrow(Electricity.movingAvg) ? r.rank : 0.0d), 
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
