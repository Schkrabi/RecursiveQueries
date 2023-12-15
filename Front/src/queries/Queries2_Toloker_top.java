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
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("tolokerTop")
public class Queries2_Toloker_top extends Queries2 {
	
	private  double APPROVED_RATE_MAX = 0.5d;
	
	@QueryParameter("approvedRateMax")
	public void setApprovedRateMax(String approvedRateMax) {
		this.APPROVED_RATE_MAX = Double.parseDouble(approvedRateMax);
	}
	
	@QueryParameterGetter("approvedRateMax")
	public String getApprovedRateMax() {
		return Double.toString(APPROVED_RATE_MAX);
	}
	
	private  int BANNED = 1;
	
	@QueryParameter("banned")
	public void setBanned(String banned) {
		this.BANNED = Integer.parseInt(banned);
	}
	
	@QueryParameterGetter("banned")
	public String getBanned() {
		return Integer.toString(this.BANNED);
	}
	
	private double INITIAL_APPROVED_RATE = 0.0d;
	
	@QueryParameter("initialApprovedRate")
	public void setInitialApprovedRate(String initialApprovedRate) {
		this.INITIAL_APPROVED_RATE = Double.parseDouble(initialApprovedRate);
	}
	
	@QueryParameterGetter("initialApprovedRate")
	public String getInitialApprovedRate() {
		return Double.toString(this.INITIAL_APPROVED_RATE);
	}
	
	private final BiFunction<Object, Object, Double> approvedRateSimilarity = LinearSimilarity.doubleSimilarityUntil(APPROVED_RATE_MAX);

	public Queries2_Toloker_top(Class<? extends LazyRecursive> algorithm, AlgorithmMonitor monitor) {
		super(algorithm, monitor);
	}

	@Override
	protected String algIdentificator() {
		return new StringBuilder()
				.append("Toloker")
				.append("Top")
				.toString();
	}

	@Override
	protected Function<Table, LazyExpression> initialProvider() throws Exception {
		return (Table iTable) -> {
			try {
				return LazyProjection.factory(
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
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> new LazyFacade(iTable);
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression exp = null;
		
		exp = LazyRestriction.factory(
				iTable, 
				(rq.common.table.Record r) -> {
					int sourceBanned = (int)r.getNoThrow(Toloker.sourceBanned);
					int targetBanned = (int)r.getNoThrow(Toloker.targetBanned);
					if(sourceBanned != this.BANNED || targetBanned  != this.BANNED) {
						return 0.0d;
					}
					
					return r.rank;
				});
		
		return exp;
	}

}
