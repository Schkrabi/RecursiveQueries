package queries;

import java.util.function.BiFunction;

import annotations.CallingArg;
import data.Toloker;
import rq.common.algorithms.LazyRecursiveTopK;
import rq.common.algorithms.LazyRecursiveUnrestricted;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.Schema;
import rq.common.tools.AlgorithmMonitor;
import rq.common.tools.Counter;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.table.TopKTable;

@CallingArg("toloker_unrtop")
public class Queries_Toloker_UnrTopK extends Queries{
	
	private final double APPROVED_RATE_MAX = 0.5d;
	private final int BANNED = 1;
	private final double INITIAL_APPROVED_RATE = 0.0d;
	private final int K = 100;
	
	private final BiFunction<Object, Object, Double> approvedRateSimilarity = LinearSimilarity.doubleSimilarityUntil(APPROVED_RATE_MAX);
	
	public Queries_Toloker_UnrTopK(Algorithm algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}
	
	public static Queries factory(Algorithm algorithm, AlgorithmMonitor monitor) {
		return new Queries_Toloker_UnrTopK(algorithm, monitor);
	}
	
	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("toloker")
				.append("_unrXtop")
				.append("_approvedRateMax=").append(APPROVED_RATE_MAX)
				.append("_banned=").append(BANNED)
				.append("_K=").append(K)
				.append("_initalApprovedRate=").append(INITIAL_APPROVED_RATE)
				.toString();
	}

	private LazyExpression prepareData(LazyExpression iTable) {
		LazyExpression exp = null;
		
		exp = LazyRestriction.factory(
				iTable, 
				(rq.common.table.Record r) -> {
					int sourceBanned = (int)r.getNoThrow(Toloker.sourceBanned);
					int targetBanned = (int)r.getNoThrow(Toloker.targetBanned);
					if(sourceBanned == 0 || targetBanned == 0) {
						return 0.0d;
					}
					
					return r.rank;
				});
		
		return exp;
	}
	
	@Override
	protected LazyExpression prepareUnrestricted(LazyExpression iTable) {
		return this.prepareData(iTable);
	}

	@Override
	protected LazyExpression prepareTopK(LazyExpression iTable) {
		return this.prepareData(iTable);
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
										r -> this.approvedRateSimilarity.apply((double)r.getNoThrow(Toloker.sourceApprovedRate), 0.0d)),
								new Projection.To(Toloker.source, Toloker.source),
								new Projection.To(Toloker.sourceApprovedRate, Toloker.approvedRate),
								new Projection.To(Toloker.sourceRejectedRate, Toloker.rejectedRate),
								new Projection.To(Toloker.sourceExpiredRate, Toloker.expiredRate),
								new Projection.To(Toloker.sourceSkippedRate, Toloker.skippedRate),
								new Projection.To(Toloker.sourceEductation, Toloker.education),
								new Projection.To(Toloker.sourceEnglishProfile, Toloker.englishProfile),
								new Projection.To(Toloker.sourceEnglishTested, Toloker.englishTested),
								new Projection.To(Toloker.sourceBanned, Toloker.banned)),
						(Table t) ->
							{
								try {
									return LazyProjection.factory(
											LazyJoin.factory(
												new LazyFacade(t), 
												LazyRestriction.factory(
														new LazyFacade(iTable),
														r -> this.approvedRateSimilarity.apply((double)r.getNoThrow(Toloker.targetApprovedRate), 0.0d)),
												new OnEquals(Toloker.source, Toloker.source),
												new OnSimilar(Toloker.education, Toloker.targetEductation, Toloker.educationSimilarity)),
											new Projection.To(Toloker.target, Toloker.source),
											new Projection.To(Toloker.targetApprovedRate, Toloker.approvedRate),
											new Projection.To(Toloker.targetRejectedRate, Toloker.rejectedRate),
											new Projection.To(Toloker.targetSkippedRate, Toloker.skippedRate),
											new Projection.To(Toloker.targetExpiredRate, Toloker.expiredRate),
											new Projection.To(Toloker.targetEductation, Toloker.education),
											new Projection.To(Toloker.targetEnglishProfile, Toloker.englishProfile),
											new Projection.To(Toloker.targetEnglishTested, Toloker.englishTested),
											new Projection.To(Toloker.targetBanned, Toloker.banned));
								} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
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
		TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveTopK.factory(
					LazyProjection.factory(
							LazyRestriction.factory(
									new LazyFacade(iTable),
									r -> this.approvedRateSimilarity.apply((double)r.getNoThrow(Toloker.sourceApprovedRate), 0.0d)),
							new Projection.To(Toloker.source, Toloker.source),
							new Projection.To(Toloker.sourceApprovedRate, Toloker.approvedRate),
							new Projection.To(Toloker.sourceRejectedRate, Toloker.rejectedRate),
							new Projection.To(Toloker.sourceExpiredRate, Toloker.expiredRate),
							new Projection.To(Toloker.sourceSkippedRate, Toloker.skippedRate),
							new Projection.To(Toloker.sourceEductation, Toloker.education),
							new Projection.To(Toloker.sourceEnglishProfile, Toloker.englishProfile),
							new Projection.To(Toloker.sourceEnglishTested, Toloker.englishTested),
							new Projection.To(Toloker.sourceBanned, Toloker.banned)), 
					(Table t) ->
					{
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
										new LazyFacade(t), 
										LazyRestriction.factory(
												new LazyFacade(iTable),
												r -> this.approvedRateSimilarity.apply((double)r.getNoThrow(Toloker.targetApprovedRate), 0.0d)),
										new OnEquals(Toloker.source, Toloker.source),
										new OnSimilar(Toloker.education, Toloker.targetEductation, Toloker.educationSimilarity)),
									new Projection.To(Toloker.target, Toloker.source),
									new Projection.To(Toloker.targetApprovedRate, Toloker.approvedRate),
									new Projection.To(Toloker.targetRejectedRate, Toloker.rejectedRate),
									new Projection.To(Toloker.targetSkippedRate, Toloker.skippedRate),
									new Projection.To(Toloker.targetExpiredRate, Toloker.expiredRate),
									new Projection.To(Toloker.targetEductation, Toloker.education),
									new Projection.To(Toloker.targetEnglishProfile, Toloker.englishProfile),
									new Projection.To(Toloker.targetEnglishTested, Toloker.englishTested),
									new Projection.To(Toloker.targetBanned, Toloker.banned));
						} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
							throw new RuntimeException(e);
						}
					}, 
					K, 
					this.monitor);
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
				(rq.common.table.Record r) -> r.rank,
				(Schema s, Integer k) -> TopKTable.factory(s, K));
		
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
