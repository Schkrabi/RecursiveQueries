package rq.estimations.main;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import rq.common.estimations.CenterOfGravityRepresentativeProvider;
import rq.common.estimations.GlobalCenterRepresentativeProvider;
import rq.common.estimations.IEstimation;
import rq.common.estimations.InternalParetPostprocessProvider_intervalHist;
import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.IntervalParetHybridEstimation;
import rq.common.estimations.ParetWeightedEstimation_sampledHist;
import rq.common.estimations.InternalParetPostprocessProvider_sampledHist;
import rq.common.estimations.RandomEstimation;
import rq.common.estimations.WeightedCompositeEstimation;
import rq.common.estimations.WeighterdParetPostprocessProvider_intervalHist;
import rq.common.estimations.WeighterdParetPostprocessProvider_sampledHist;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;

/** Lists and carries out all estimates for parameters */
public class NumericalEstimations {
	//Params
	public final Path dataPath;
	public final Attribute attribute;
	public final int slice;
	public final BiFunction<Object, Object, Double> similarity;
	public final Collection<Integer> intervals;
	public final Collection<Integer> numOfConsideredValues;
	public final Collection<Double> paretRatios;
	
	//Derived
	public final SampledHistogram sHist;
	public final Collection<EquidistantHistogram> eqds;
	public final Collection<EquinominalHistogram> eqns;
	
	public NumericalEstimations(
			Path dataPath,
			Attribute attribute,
			int slice,
			BiFunction<Object, Object, Double> similarity,
			Collection<Integer> intervals,
			Collection<Integer> numOfConsideredValues,
			Collection<Double> paretRatios) {
		this.dataPath = dataPath;
		this.attribute = attribute;
		this.slice = slice;
		this.similarity = similarity;
		this.intervals = intervals;
		this.numOfConsideredValues = numOfConsideredValues;
		this.paretRatios = paretRatios;
		
		this.sHist = ResourceLoader.instance().getOrLoadSampledHistogram(dataPath, attribute);
		this.eqds = ResourceLoader.instance().getOrLoadEqdHistograms(dataPath, attribute, intervals);
		this.eqns = ResourceLoader.instance().getOrLoadEqnHistograms(dataPath, attribute, intervals);
	}
	
	private List<IEstimation> _estimations = null;
	
	protected List<IEstimation> getEstmations(){
		if(_estimations == null) {
			_estimations = this.generateEstimations();
		}
		return _estimations;
	}
	
	private List<IEstimation> generateEstimations() {
		var l = new ArrayList<IEstimation>();
		var mcv = ResourceLoader.instance().getOrLoadMCV(this.dataPath, this.attribute);
		
//		l.add(new ParetWeightedEstimation_sampledHist(slice, similarity, sHist.valuesCount(), sHist));
		l.add(new RandomEstimation(slice, sHist.tableSize()));
		
		for (var i : this.eqds) {
//			l.add(IntervalEstimation.eqd(slice, similarity, i));
//			l.add(GlobalCenterRepresentativeProvider.eqdC(slice, similarity, i));
//			l.add(new IntervalParetHybridEstimation(slice, i, mcv.mostCommon(20), similarity, i.center()));
//			l.add(IntervalParetHybridEstimation.unknownConstant(slice, i, mcv, similarity));

			for (var n : this.numOfConsideredValues) {
//				for(var r : this.paretRatios) {
//					l.add(WeighterdParetPostprocessProvider_sampledHist.eqdGps(slice, similarity, i, n, r, sHist));
//					l.add(WeighterdParetPostprocessProvider_sampledHist.eqdCGps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqdIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqdCIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqdGpIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqdCGpIps(slice, similarity, i, n, r, sHist));
					
//					l.add(WeighterdParetPostprocessProvider_intervalHist.eqdGpi(slice, similarity, i, n, r));
//					l.add(WeighterdParetPostprocessProvider_intervalHist.eqdCGpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqdIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqdCIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqdGpIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqdCGpIpi(slice, similarity, i, n, r));
//				}
				
//				l.add(new WeightedCompositeEstimation(slice, i, IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER, n, similarity));
//				l.add(new WeightedCompositeEstimation(slice, i, new CenterOfGravityRepresentativeProvider(mcv), n, similarity));
			}
		}

		for (var i : this.eqns) {
//			l.add(IntervalEstimation.eqn(slice, similarity, i));
//			l.add(GlobalCenterRepresentativeProvider.eqnC(slice, similarity, i));
//			l.add(new IntervalParetHybridEstimation(slice, i, mcv.mostCommon(20), similarity, i.center()));
//			l.add(IntervalParetHybridEstimation.unknownConstant(slice, i, mcv, similarity));
			
			for (var n : this.numOfConsideredValues) {
//				for(var r : this.paretRatios) {
//					l.add(WeighterdParetPostprocessProvider_sampledHist.eqnGps(slice, similarity, i, n, r, sHist));
//					l.add(WeighterdParetPostprocessProvider_sampledHist.eqnCGps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqnIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqnCIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqnGpIps(slice, similarity, i, n, r, sHist));
//					l.add(InternalParetPostprocessProvider_sampledHist.eqnCGpIps(slice, similarity, i, n, r, sHist));
					
//					l.add(WeighterdParetPostprocessProvider_intervalHist.eqnGpi(slice, similarity, i, n, r));
//					l.add(WeighterdParetPostprocessProvider_intervalHist.eqnCGpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqnIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqnCIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqnGpIpi(slice, similarity, i, n, r));
//					l.add(InternalParetPostprocessProvider_intervalHist.eqnCGpIpi(slice, similarity, i, n, r));
//				}
				
//				l.add(new WeightedCompositeEstimation(slice, i, IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER, n, similarity));
//				l.add(new WeightedCompositeEstimation(slice, i, new CenterOfGravityRepresentativeProvider(mcv), n, similarity));
			}
		}
		return l;
	}
	
	

}
