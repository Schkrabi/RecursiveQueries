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
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.AbstractJoin;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyProjection;
import rq.common.operators.Projection;
import rq.common.operators.LazySelection;
import rq.common.restrictions.Equals;
import rq.common.restrictions.InfimumAnd;
import rq.common.onOperators.Constant;
import rq.common.similarities.LinearSimilarity;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.tools.AlgorithmMonitor;

@CallingArg("tolokerTra")
public class Queries2_Toloker_tra extends Queries2 {	
	private int _startNode = 64;//9;
	
	@QueryParameter("startNode")
	public void setStartNode(String source) {
		this._startNode = Integer.parseInt(source);
	}
	
	@QueryParameterGetter("startNode")
	public String getStartNode() {
		return Integer.toString(this._startNode);
	}
	
	private double _approvedRateSimilarity = 1.0d;
	private BiFunction<Object, Object, Double> approvedRateSimilarity = 
			LinearSimilarity.doubleSimilarityUntil(_approvedRateSimilarity);
	
	@QueryParameter("approvedRateSimilarity")
	public void setApprovedRateSimilarity(String rateSimilarity) {
		this._approvedRateSimilarity = Double.parseDouble(rateSimilarity);
		this.approvedRateSimilarity = 
				LinearSimilarity.doubleSimilarityUntil(_approvedRateSimilarity);
	}
	
	@QueryParameterGetter("approvedRateSimilarity")
	public String getApprovedRateSimilarity() {
		return Double.toString(this._approvedRateSimilarity);
	}
	
	private int _isBanned = 0;
	
	@QueryParameter("isBanned")
	public void setIsBanned(String isBanned) {
		this._isBanned = Integer.parseInt(isBanned);
	}
	
	@QueryParameterGetter("isBanned")
	public String getIsBanned() {
		return Integer.toString(this._isBanned);
	}
	
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
			return LazyProjection.factory(
					new LazySelection(
							new LazyFacade(iTable),
							new Equals(Toloker.source, new Constant<Integer>(_startNode))),
					new Projection.To(Toloker.source, Toloker.source),
					new Projection.To(Toloker.sourceApprovedRate, Toloker.approvedRate),
//					new Projection.To(Toloker.sourceRejectedRate, Toloker.rejectedRate),
//					new Projection.To(Toloker.sourceExpiredRate, Toloker.expiredRate),
//					new Projection.To(Toloker.sourceSkippedRate, Toloker.skippedRate),
					new Projection.To(Toloker.sourceEductation, Toloker.education)
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
						new Projection.To(Toloker.targetApprovedRate, Toloker.approvedRate),
//						new Projection.To(Toloker.targetRejectedRate, Toloker.rejectedRate),
//						new Projection.To(Toloker.targetSkippedRate, Toloker.skippedRate),
//						new Projection.To(Toloker.targetExpiredRate, Toloker.expiredRate),
						new Projection.To(Toloker.targetEductation, Toloker.education)
//						new Projection.To(Toloker.targetEnglishProfile, Toloker.englishProfile),
//						new Projection.To(Toloker.targetEnglishTested, Toloker.englishTested),
//						new Projection.To(Toloker.targetBanned, Toloker.banned)
						);
			};
		};
	}

	@Override
	protected Function<Table, LazyExpression> postprocessProvider() throws Exception {
		return (Table iTable) -> {
			return LazyProjection.factory( 
				LazyJoin.factory(
					new LazyFacade(MemoryTable.of(this.rec)), 
					new LazyFacade(iTable),
					new OnSimilar(
							Toloker.sourceApprovedRate, 
							Toloker.approvedRate, 
							this.approvedRateSimilarity)),
				new Projection.To(AbstractJoin.right(Toloker.source), Toloker.source));
		};
	}

	@Override
	public LazyExpression preprocess(LazyExpression iTable) {
		LazyExpression exp = null;
		
		//Selects the original source record used later in post processing
		this.rec =
				new LazySelection(iTable, new Equals(Toloker.source, new Constant<Integer>(this._startNode)))
				.next();
		
		exp = new LazySelection(
				iTable,
				new InfimumAnd(	new Equals(Toloker.sourceBanned, new Constant<Integer>(this._isBanned)),
								new Equals(Toloker.targetBanned, new Constant<Integer>(this._isBanned))));
		
		return exp;
	}

}
