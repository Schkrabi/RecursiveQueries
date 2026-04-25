package rq.estimations.contracts;

import java.util.ArrayList;
import java.util.Collection;

import rq.common.similarities.ISimilarity;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.estimations.framework.ResourceLoader;
import rq.estimations.main.QueryHistogramHolder.RankHistogramInfo;

public class ConstantRestrictionExperimentContract {
	private final RankHistogramInfo<Double> rsltInfo;
	private final ISimilarity<Double> similarity;
	private final Collection<String> estSignatures;
	private MostCommonValues<Double> mcv;
	private EquidistantHistogram<Double> eqd;
	private EquinominalHistogram<Double> eqn;
	
	public ConstantRestrictionExperimentContract(
			RankHistogramInfo<Double> rsltInfo,
			ISimilarity<Double> similarity,
			Collection<String> estSignatures) {
		this.rsltInfo = rsltInfo;
		this.similarity = similarity;
		this.estSignatures = new ArrayList<String>(estSignatures);
	}
	
	/**
	 * @return the estSignatures
	 */
	public Collection<String> getEstSignatures() {
		return estSignatures;
	}
	/**
	 * @return the similarity
	 */
	public ISimilarity<Double> getSimilarity() {
		return similarity;
	}
	/**
	 * @return the rsltInfo
	 */
	public RankHistogramInfo<Double> getRsltInfo() {
		return rsltInfo;
	}
	/**
	 * @return the mcv
	 */
	public MostCommonValues<Double> getMcv() {
		if(this.mcv == null) {
			this.mcv = ResourceLoader.instance().getOrLoadMCV(
					this.rsltInfo.queryInfo.dataPath, 
					this.rsltInfo.queryInfo.attribute);
		}
		return mcv;
	}
	/**
	 * @return the eqn
	 */
	public EquinominalHistogram<Double> getEqn() {
		if(this.eqn == null) {
			this.eqn = ResourceLoader.instance()
					.getOrLoadAllEqnHistograms(
							this.rsltInfo.queryInfo.dataPath, 
							this.rsltInfo.queryInfo.attribute)
					.stream()
					.findAny().get();
		}
		return eqn;
	}
	/**
	 * @return the eqd
	 */
	public EquidistantHistogram<Double> getEqd() {
		if(this.eqd == null) {
			this.eqd = ResourceLoader.instance()
					.getOrLoadAllEqdHistograms(this.rsltInfo.queryInfo.dataPath, this.rsltInfo.queryInfo.attribute)
					.stream()
					.findAny().get();
		}
		return eqd;
	}
}
