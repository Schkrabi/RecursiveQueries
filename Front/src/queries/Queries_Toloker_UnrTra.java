package queries;

import java.util.function.BiFunction;

import data.Toloker;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.Goguen;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.AbstractJoin;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveTransformed;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.tools.Counter;

@CallingArg("toloker_unrtra")
public class Queries_Toloker_UnrTra extends Queries {
	
	private final double APPROVED_RATE_MAX = 0.5d;
	private final int BANNED = 1;
	private final int SOURCE = 9;//4634;
	private final int K = 1000;
	
	private final double RATE_SIMILARITY = 1.0d;
	private final BiFunction<Object, Object, Double> rateSimilarity = LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY);
	
	private rq.common.table.Record rec = null;

	protected Queries_Toloker_UnrTra(Algorithm algorithm, Counter counter) {
		super(algorithm, counter);
	}
	
	public static Queries factory(Algorithm algorithm, Counter counter) {
		return new Queries_Toloker_UnrTra(algorithm, counter);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("toloker")
				.append("_unrXtra")
				.append("_approvedRateMax=").append(APPROVED_RATE_MAX)
				.append("_banned=").append(BANNED)
				.append("_source=").append(SOURCE)
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
		
		BiFunction<Object, Object, Double> approvedRateSimilarity = LinearSimilarity.doubleSimilarityUntil(APPROVED_RATE_MAX);
		
		exp = LazyRestriction.factory(
				iTable, 
				(rq.common.table.Record r) -> {
					double approvedRate = (double)r.getNoThrow(Toloker.approvedRate);
					int banned = (int)r.getNoThrow(Toloker.banned);
					if(banned == 0) {
						return 0.0d;
					}
					
					return approvedRateSimilarity.apply(approvedRate, 0.0d);
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
							new Projection.To(Toloker.approvedRate, Toloker.approvedRate),
							new Projection.To(Toloker.rejectedRate, Toloker.rejectedRate),
							new Projection.To(Toloker.expiredRate, Toloker.expiredRate),
							new Projection.To(Toloker.skippedRate, Toloker.skippedRate),
							new Projection.To(Toloker.education, Toloker.education),
							new Projection.To(Toloker.englishProfile, Toloker.englishProfile),
							new Projection.To(Toloker.englishTested, Toloker.englishTested),
							new Projection.To(Toloker.banned, Toloker.banned)),
							(Table t) ->
								{
									try {
										return LazyProjection.factory(
												LazyJoin.factory(
													new LazyFacade(t), 
													new LazyFacade(iTable), 
													new OnEquals(Toloker.source, Toloker.source),
													new OnSimilar(Toloker.education, Toloker.education, Toloker.educationSimilarity)),
												new Projection.To(Toloker.target, Toloker.source),
												new Projection.To(AbstractJoin.right(Toloker.approvedRate), Toloker.approvedRate),
												new Projection.To(AbstractJoin.right(Toloker.rejectedRate), Toloker.rejectedRate),
												new Projection.To(AbstractJoin.right(Toloker.skippedRate), Toloker.skippedRate),
												new Projection.To(AbstractJoin.right(Toloker.expiredRate), Toloker.expiredRate),
												new Projection.To(AbstractJoin.right(Toloker.education), Toloker.education),
												new Projection.To(AbstractJoin.right(Toloker.englishProfile), Toloker.englishProfile),
												new Projection.To(AbstractJoin.right(Toloker.englishTested), Toloker.englishTested),
												new Projection.To(AbstractJoin.right(Toloker.banned), Toloker.banned));
									} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
										throw new RuntimeException(e);
									}
								},
								this.recordCounter);
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
							new Projection.To(Toloker.approvedRate, Toloker.approvedRate),
							new Projection.To(Toloker.rejectedRate, Toloker.rejectedRate),
							new Projection.To(Toloker.expiredRate, Toloker.expiredRate),
							new Projection.To(Toloker.skippedRate, Toloker.skippedRate),
							new Projection.To(Toloker.education, Toloker.education),
							new Projection.To(Toloker.englishProfile, Toloker.englishProfile),
							new Projection.To(Toloker.englishTested, Toloker.englishTested),
							new Projection.To(Toloker.banned, Toloker.banned)), 
					(Table t) ->
					{
						try {
							return LazyProjection.factory(
									LazyJoin.factory(
										new LazyFacade(t), 
										new LazyFacade(iTable), 
										new OnEquals(Toloker.source, Toloker.source),
										new OnSimilar(Toloker.education, Toloker.education, Toloker.educationSimilarity)),
									new Projection.To(Toloker.target, Toloker.source),
									new Projection.To(AbstractJoin.right(Toloker.approvedRate), Toloker.approvedRate),
									new Projection.To(AbstractJoin.right(Toloker.rejectedRate), Toloker.rejectedRate),
									new Projection.To(AbstractJoin.right(Toloker.skippedRate), Toloker.skippedRate),
									new Projection.To(AbstractJoin.right(Toloker.expiredRate), Toloker.expiredRate),
									new Projection.To(AbstractJoin.right(Toloker.education), Toloker.education),
									new Projection.To(AbstractJoin.right(Toloker.englishProfile), Toloker.englishProfile),
									new Projection.To(AbstractJoin.right(Toloker.englishTested), Toloker.englishTested),
									new Projection.To(AbstractJoin.right(Toloker.banned), Toloker.banned));
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
									double rejectedRate = (double)r.getNoThrow(Toloker.skippedRate);
									double expiredRate = (double)r.getNoThrow(Toloker.expiredRate);
									
									double sourceApprovedRate = (double)this.rec.getNoThrow(Toloker.approvedRate);
									double sourceSkippedRate = (double)this.rec.getNoThrow(Toloker.skippedRate);
									double sourceRejectedRate = (double)this.rec.getNoThrow(Toloker.rejectedRate);
									double sourceExpiredRate = (double)this.rec.getNoThrow(Toloker.expiredRate);
									
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
					this.recordCounter);
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
					double rejectedRate = (double)r.getNoThrow(Toloker.skippedRate);
					double expiredRate = (double)r.getNoThrow(Toloker.expiredRate);
					
					double sourceApprovedRate = (double)this.rec.getNoThrow(Toloker.approvedRate);
					double sourceSkippedRate = (double)this.rec.getNoThrow(Toloker.skippedRate);
					double sourceRejectedRate = (double)this.rec.getNoThrow(Toloker.rejectedRate);
					double sourceExpiredRate = (double)this.rec.getNoThrow(Toloker.expiredRate);
					
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
