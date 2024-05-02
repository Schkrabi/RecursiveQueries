package queries;

import java.util.function.BiFunction;
import java.util.function.Function;

import annotations.CallingArg;
import annotations.QueryParameter;
import annotations.QueryParameterGetter;
import data.Toloker;
import rq.common.algorithms.LazyRecursive;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazySelection;
import rq.common.operators.Projection;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.tools.AlgorithmMonitor;
import rq.common.restrictions.Equals;
import rq.common.restrictions.InfimumAnd;

@CallingArg("tolokerTop")
public class Queries2_Toloker_top extends Queries2 {
	
	private  int _isBanned = 1;
	
	@QueryParameter("isBanned")
	public void setIsBanned(String banned) {
		this._isBanned = Integer.parseInt(banned);
	}
	
	@QueryParameterGetter("isBanned")
	public String getIsBanned() {
		return Integer.toString(this._isBanned);
	}
	
	private double _approvedRate = 0.0d;
	
	@QueryParameter("approvedRate")
	public void setApprovedRate(String initialApprovedRate) {
		this._approvedRate = Double.parseDouble(initialApprovedRate);
	}
	
	@QueryParameterGetter("approvedRate")
	public String getApprovedRate() {
		return Double.toString(this._approvedRate);
	}
	
	private double _approvedRateSimilarity = 1.0d;
	private BiFunction<Object, Object, Double> approvedRateSimilarity =
			LinearSimilarity.doubleSimilarityUntil(_approvedRateSimilarity);
	
	@QueryParameter("approvedRateSimilarity")
	public void setApprovedRateSimilarity(String arsim) {
		this._approvedRateSimilarity = Double.parseDouble(arsim);
		this.approvedRateSimilarity =
				LinearSimilarity.doubleSimilarityUntil(_approvedRateSimilarity);
	}
	
	@QueryParameterGetter("approvedRateSimilarity")
	public String getApprovedRateSimilarity() {
		return Double.toString(this._approvedRateSimilarity);
	}

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
			return LazyProjection.factory(
					new LazySelection(
							new LazyFacade(iTable),
							new Similar(Toloker.sourceApprovedRate, 
										new Constant<Double>(this._approvedRate), 
										this.approvedRateSimilarity)),
					new Projection.To(Toloker.source, Toloker.source),
//					new Projection.To(Toloker.sourceApprovedRate, Toloker.approvedRate),
//					new Projection.To(Toloker.sourceRejectedRate, Toloker.rejectedRate),
//					new Projection.To(Toloker.sourceExpiredRate, Toloker.expiredRate),
//					new Projection.To(Toloker.sourceSkippedRate, Toloker.skippedRate),
					new Projection.To(Toloker.sourceEductation, Toloker.education)//,
//					new Projection.To(Toloker.sourceEnglishProfile, Toloker.englishProfile),
//					new Projection.To(Toloker.sourceEnglishTested, Toloker.englishTested),
//					new Projection.To(Toloker.sourceBanned, Toloker.banned)
					);
		};
	}

	@Override
	protected Function<Table, Function<Table, LazyExpression>> recursiveStepProvider() throws Exception {
		return (Table iTable) -> {
			return (Table t) ->
			{
				return LazyProjection.factory(
						LazyJoin.factory(
							new LazyFacade(t), 
							new LazyFacade(iTable),
							new OnEquals(Toloker.source, Toloker.source),
							new OnSimilar(
									Toloker.education, 
									Toloker.targetEductation, 
									Toloker.educationSimilarity)),
						new Projection.To(Toloker.target, Toloker.source),
//						new Projection.To(Toloker.targetApprovedRate, Toloker.approvedRate),
//						new Projection.To(Toloker.targetRejectedRate, Toloker.rejectedRate),
//						new Projection.To(Toloker.targetSkippedRate, Toloker.skippedRate),
//						new Projection.To(Toloker.targetExpiredRate, Toloker.expiredRate),
						new Projection.To(Toloker.targetEductation, Toloker.education)//,
//						new Projection.To(Toloker.targetEnglishProfile, Toloker.englishProfile),
//						new Projection.To(Toloker.targetEnglishTested, Toloker.englishTested),
//						new Projection.To(Toloker.targetBanned, Toloker.banned)
						);
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
		
		exp = new LazySelection(
				iTable,
				new InfimumAnd(	new Equals(Toloker.sourceBanned, new Constant<Integer>(this._isBanned)),
								new Equals(Toloker.targetBanned, new Constant<Integer>(this._isBanned))));
		
		return exp;
	}

}
