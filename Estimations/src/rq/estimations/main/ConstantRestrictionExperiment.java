package rq.estimations.main;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import rq.common.estimations.ConstantRepresentativeProvider;
import rq.common.estimations.IEstimation;
import rq.common.estimations.IntervalParetHybridEstimation;
import rq.common.estimations.ParPrecConst;
import rq.common.similarities.LinearSimilarity;
import rq.estimations.main.QueryHistogramHolder.RankHistogramInfo;

/** Holds all estimations against constant against given query result*/
public class ConstantRestrictionExperiment {

	private List<IEstimation> _estimations;
	public final RankHistogramInfo rsltInfo;
	private final BiFunction<Object, Object, Double> similarity;
	private final Collection<Integer> values;
	
	public ConstantRestrictionExperiment(
			RankHistogramInfo rsltInfo,
			Collection<Integer> values) {
		this.rsltInfo = rsltInfo;
		this.values = values;
		this.similarity = LinearSimilarity.doubleSimilarityUntil(this.rsltInfo.queryInfo.similarUntil);
	}

	/**Gets the estimations, cached*/
	public List<IEstimation> estimations(){
		if(this._estimations == null) {
			this._estimations = this.doEstimations();		
		}
		return this._estimations;
	}
	
	/** creates all estimation objects*/
	protected List<IEstimation> doEstimations(){
		var mcv = 
				ResourceLoader.instance().getOrLoadMCV(this.rsltInfo.queryInfo.dataPath, this.rsltInfo.queryInfo.attribute);
		
		var l = new ArrayList<IEstimation>();
		l.add(new ParPrecConst(
			this.rsltInfo.queryInfo.attribute,
			this.rsltInfo.slice, 
			this.rsltInfo.queryInfo.constant, 
			this.similarity,
//			mcv.mostCommon(5)
			mcv.mostCommon(20)
			));
		var eqd = ResourceLoader.instance()
				.getOrLoadAllEqdHistograms(this.rsltInfo.queryInfo.dataPath, this.rsltInfo.queryInfo.attribute)
				.stream()
				.reduce((e1, e2) -> e1.n > e2.n ? e1 : e2).get();
		l.add(ConstantRepresentativeProvider.eqdK(this.rsltInfo.slice, similarity, eqd, this.rsltInfo.queryInfo.constant));
		l.add(IntervalParetHybridEstimation.knownConstant(
				this.rsltInfo.slice, 
				eqd, 
				mcv, 
				similarity, 
				this.rsltInfo.queryInfo.constant));
		
		var eqn = ResourceLoader.instance()
				.getOrLoadAllEqnHistograms(this.rsltInfo.queryInfo.dataPath, this.rsltInfo.queryInfo.attribute)
				.stream()
				.reduce((e1, e2) -> e1.n > e2.n ? e1 : e2).get();
		l.add(ConstantRepresentativeProvider.eqnK(this.rsltInfo.slice, similarity, eqn, this.rsltInfo.queryInfo.constant));
		l.add(IntervalParetHybridEstimation.knownConstant(
				this.rsltInfo.slice, 
				eqn, 
				mcv, 
				similarity, 
				this.rsltInfo.queryInfo.constant));
		
		
		return l;
	}
}
