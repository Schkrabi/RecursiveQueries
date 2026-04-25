package rq.estimations.contracts;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;

import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.estimations.framework.ResourceLoader;

public class NumericalEstimationsContract {
	public final Path dataPath;
	public final Attribute<Double> attribute;
	public final int slice;
	public final rq.common.similarities.ISimilarity<Double> similarity;
	public final Collection<String> signatures;
	
	private SampledHistogram<Double> sHist = null;
	private EquidistantHistogram<Double> eqd = null;
	private EquinominalHistogram<Double> eqn = null;
	private MostCommonValues<Double> mcv = null;
	
	public NumericalEstimationsContract(
			Path dataPath,
			Attribute<Double> attribute,
			int slice,
			rq.common.similarities.ISimilarity<Double> similarity,
			Collection<String> signatures) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.slice = slice;
		this.similarity = similarity;
		this.signatures = signatures;
	}
	
	public SampledHistogram<Double> getSHist(){
		if(this.sHist == null) {
			this.sHist = ResourceLoader.instance().getOrLoadSampledHistogram(dataPath, attribute);
		}
		return this.sHist;
	}
	
	public EquidistantHistogram<Double> getEqd(){
		if(this.eqd == null) {
			this.eqd = ResourceLoader.instance().getOrLoadAllEqdHistograms(this.dataPath, this.attribute)
					.stream().findAny().get();
		}
		return this.eqd;
	}
	
	public EquinominalHistogram<Double> getEqn(){
		if(this.eqn == null) {
			this.eqn = ResourceLoader.instance().getOrLoadAllEqnHistograms(this.dataPath, this.attribute)
					.stream().findAny().get();
		}
		return this.eqn;
	}
	
	public MostCommonValues<Double> getMcv(){
		if(this.mcv == null) {
			this.mcv = ResourceLoader.instance().getOrLoadMCV(this.dataPath, this.attribute);
		}
		return this.mcv;
	}
}
