package queries;

import java.util.function.BiFunction;

import annotations.CallingArg;
import data.Toloker;
import rq.common.algorithms.LazyRecursiveTransformed;
import rq.common.algorithms.LazyRecursiveUnrestricted;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Goguen;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("toloker_unrtra")
public class Queries_Toloker_UnrTra extends Queries {
	
	private final double APPROVED_RATE_MAX = 0.5d;
	private final int BANNED = 1;
	private final int SOURCE = 9;
	private final int K = 1000;
	
	private final double RATE_SIMILARITY = 1.0d;
	private final BiFunction<Object, Object, Double> rateSimilarity = LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY);
	
	private rq.common.table.Record rec = null;

	protected Queries_Toloker_UnrTra(Algorithm algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}
	
	public static Queries factory(Algorithm algorithm, AlgorithmMonitor monitor) {
		return new Queries_Toloker_UnrTra(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("toloker")
				.append("_unrXtra")
				.append("_approvedRateMax=").append(APPROVED_RATE_MAX)
				.append("_banned=").append(BANNED)
				.append("_source=").append(SOURCE)
				.append("_K=").append(K)
				.append("_rateSimilarity=").append(RATE_SIMILARITY)
				.toString();
	}
	
	private LazyExpression prepareData(LazyExpression iTable) {
		LazyExpression exp = null;
		
		//Selects the original source record used later in post processing
		this.rec =
				LazyRestriction.factory(
						iTable, 
						(rq.common.table.Record r) -> (int)r.getNoThrow(Toloker.source) == SOURCE ? 1.0d : 0.0d)
				.next();
		
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
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected LazyExpression prepareTransformed(LazyExpression iTable) {
		return this.prepareData(iTable);
	}

	@Override
	protected TabularExpression queryUnterstricted(Table iTable) {
		TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveUnrestricted.factory(
					LazyProjection.factory(
							LazyRestriction.factory(
									new LazyFacade(iTable), 
									r -> (int)r.getNoThrow(Toloker.source) == SOURCE ? r.rank : 0.0d),
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
										new LazyFacade(iTable), 
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
		throw this.algorithmNotSupportedException();
	}

	@Override
	protected TabularExpression queryTransformed(Table iTable) {
		TabularExpression exp = null;
		
		try {
			exp = LazyRecursiveTransformed.factory(
					LazyProjection.factory(
							LazyRestriction.factory(
									new LazyFacade(iTable), 
									r -> (int)r.getNoThrow(Toloker.source) == SOURCE ? r.rank : 0.0d),
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
										new LazyFacade(iTable), 
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
					(rq.common.table.Record re) -> {
						LazyExpression lexp = null;
						
						lexp = LazyRestriction.factory(
								new LazyFacade(MemoryTable.of(re)), 
								(rq.common.table.Record r) -> {
									double approvedRate = (double)r.getNoThrow(Toloker.approvedRate);
									double skippedRate = (double)r.getNoThrow(Toloker.skippedRate);
									double rejectedRate = (double)r.getNoThrow(Toloker.rejectedRate);
									double expiredRate = (double)r.getNoThrow(Toloker.expiredRate);
									
									double sourceApprovedRate = (double)this.rec.getNoThrow(Toloker.sourceApprovedRate);
									double sourceSkippedRate = (double)this.rec.getNoThrow(Toloker.sourceSkippedRate);
									double sourceRejectedRate = (double)this.rec.getNoThrow(Toloker.sourceRejectedRate);
									double sourceExpiredRate = (double)this.rec.getNoThrow(Toloker.sourceExpiredRate);
									
									return Goguen.PRODUCT.apply(
											rateSimilarity.apply(approvedRate, sourceApprovedRate), 
											Goguen.PRODUCT.apply(
													rateSimilarity.apply(skippedRate, sourceSkippedRate), 
													Goguen.PRODUCT.apply(
															rateSimilarity.apply(rejectedRate, sourceRejectedRate), 
															Goguen.PRODUCT.apply(
																	rateSimilarity.apply(expiredRate, sourceExpiredRate), 
																	r.rank))));
								});
						
						return lexp;
					}, 
					this.monitor);
		} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
			throw new RuntimeException(e);
		}
		
		return exp;
	}

	@Override
	protected TabularExpression postprocessUnrestricted(Table iTable) {
		TabularExpression exp = null;
		
		exp = new Restriction(
				iTable,
				(rq.common.table.Record r) -> {
					double approvedRate = (double)r.getNoThrow(Toloker.approvedRate);
					double skippedRate = (double)r.getNoThrow(Toloker.skippedRate);
					double rejectedRate = (double)r.getNoThrow(Toloker.rejectedRate);
					double expiredRate = (double)r.getNoThrow(Toloker.expiredRate);
					
					double sourceApprovedRate = (double)this.rec.getNoThrow(Toloker.sourceApprovedRate);
					double sourceSkippedRate = (double)this.rec.getNoThrow(Toloker.sourceSkippedRate);
					double sourceRejectedRate = (double)this.rec.getNoThrow(Toloker.sourceRejectedRate);
					double sourceExpiredRate = (double)this.rec.getNoThrow(Toloker.sourceExpiredRate);
					
					return Goguen.PRODUCT.apply(
							rateSimilarity.apply(approvedRate, sourceApprovedRate), 
							Goguen.PRODUCT.apply(
									rateSimilarity.apply(skippedRate, sourceSkippedRate), 
									Goguen.PRODUCT.apply(
											rateSimilarity.apply(rejectedRate, sourceRejectedRate), 
											Goguen.PRODUCT.apply(
													rateSimilarity.apply(expiredRate, sourceExpiredRate), 
													r.rank))));
				},
				(rq.common.table.Schema s, Integer k) -> rq.common.table.TopKTable.factory(s, K));
		
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
