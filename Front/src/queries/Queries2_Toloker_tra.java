package queries;

import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Toloker;
import rq.common.algorithms.LazyRecursive;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.latices.Goguen;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("tolokerTra")
public class Queries2_Toloker_tra extends Queries2 {
	
	private  double APPROVED_RATE_MAX = 0.5d;
	
	@QueryParameter("approvedRateMax")
	public void setApprovedRateMax(String approvedRateMax) {
		this.APPROVED_RATE_MAX = Double.parseDouble(approvedRateMax);
	}
	
	@QueryParameterGetter("approvedRateMax")
	public String getApprovedRateMax() {
		return Double.toString(APPROVED_RATE_MAX);
	}
	
	private int BANNED = 1;
	
	@QueryParameter("banned")
	public void setBanned(String banned) {
		this.BANNED = Integer.parseInt(banned);
	}
	
	@QueryParameterGetter("banned")
	public String getBanned() {
		return Integer.toString(this.BANNED);
	}
	
	private int SOURCE = 9;
	
	@QueryParameter("source")
	public void setSource(String source) {
		this.SOURCE = Integer.parseInt(source);
	}
	
	@QueryParameterGetter("source")
	public String getSource() {
		return Integer.toString(this.SOURCE);
	}
	
	private double RATE_SIMILARITY = 1.0d;
	
	@QueryParameter("rateSimilarity")
	public void setRateSimilarity(String rateSimilarity) {
		this.RATE_SIMILARITY = Double.parseDouble(rateSimilarity);
	}
	
	@QueryParameterGetter("rateSimilarity")
	public String getRateSimilarity() {
		return Double.toString(this.RATE_SIMILARITY);
	}
	
	private final BiFunction<Object, Object, Double> rateSimilarity = LinearSimilarity.doubleSimilarityUntil(RATE_SIMILARITY);
	
	private rq.common.table.Record rec = null;

	public Queries2_Toloker_tra(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Toloker")
				.append("Tra")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			try {
				return LazyProjection.factory(
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
						new Projection.To(Toloker.sourceBanned, Toloker.banned));
			} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
				throw new RuntimeException(e);
			}
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) ->
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
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> {
			return LazyRestriction.factory(
					new LazyFacade(iTable), 
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
		};
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
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
					if(sourceBanned != this.BANNED || targetBanned != this.BANNED) {
						return 0.0d;
					}
					
					return r.rank;
				});
		
		return exp;
	}

}
