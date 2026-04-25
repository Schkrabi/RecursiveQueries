package rq.estimations.framework;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import rq.common.estimations.CenterOfGravityRepresentativeProvider;
import rq.common.estimations.GlobalCenterRepresentativeProvider;
import rq.common.estimations.IEstimation;
import rq.common.estimations.InternalParetPostprocessProvider_intervalHist;
import rq.common.estimations.InternalParetPostprocessProvider_sampledHist;
import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.IntervalParetHybridEstimation;
import rq.common.estimations.ParetWeightedEstimation_sampledHist;
import rq.common.estimations.RandomEstimation;
import rq.common.estimations.WeightedCompositeEstimation;
import rq.common.estimations.WeighterdParetPostprocessProvider_intervalHist;
import rq.common.estimations.WeighterdParetPostprocessProvider_sampledHist;
import rq.estimations.contracts.NumericalEstimationsContract;

/** Lists and carries out all estimates for parameters */
public class NumericalEstimations {
	
	private final NumericalEstimationsContract contract;
	
	public NumericalEstimations(
			NumericalEstimationsContract contract) {
		this.contract = contract;
	}
	
	private List<IEstimation> _estimations = null;
	
	protected List<IEstimation> getEstmations(){
		if(_estimations == null) {
			_estimations = this.generateEstimations();
		}
		return _estimations;
	}
	
	private List<IEstimation> generateEstimations() {
		return this.contract.signatures.stream()
				.map(sig -> getEstProviders().get(sig).apply(this.contract))
				.toList();
	}
	
	private static Map<String, Function<NumericalEstimationsContract, IEstimation>> estProviders = null;
	
	private static Map<String, Function<NumericalEstimationsContract, IEstimation>> getEstProviders(){
		if(estProviders == null) {
			estProviders = new HashMap<>();
			estProviders.put("rand", cnt -> new RandomEstimation(cnt.slice, cnt.getSHist().tableSize()));
			estProviders.put("wghpars", cnt -> new ParetWeightedEstimation_sampledHist(cnt.slice, cnt.similarity, cnt.getSHist().valuesCount(), cnt.getSHist()));
			estProviders.put("eqd", cnt -> IntervalEstimation.eqd(cnt.slice, cnt.similarity, cnt.getEqd()));
			estProviders.put("eqdc", cnt -> GlobalCenterRepresentativeProvider.eqdC(cnt.slice, cnt.similarity, cnt.getEqd()));
			estProviders.put("heqdppc", cnt -> IntervalParetHybridEstimation.unknownConstant(cnt.slice, cnt.getEqd(), cnt.getMcv(), cnt.similarity));
			estProviders.put("eqdgps", cnt -> WeighterdParetPostprocessProvider_sampledHist.eqdGps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdcgps", cnt -> WeighterdParetPostprocessProvider_sampledHist.eqdCGps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdips", cnt -> InternalParetPostprocessProvider_sampledHist.eqdIps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdcips", cnt -> InternalParetPostprocessProvider_sampledHist.eqdCIps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdgpips", cnt -> InternalParetPostprocessProvider_sampledHist.eqdGpIps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdcgpips", cnt -> InternalParetPostprocessProvider_sampledHist.eqdCGpIps(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqdgpi", cnt -> WeighterdParetPostprocessProvider_intervalHist.eqdGpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("eqdcgpi", cnt -> WeighterdParetPostprocessProvider_intervalHist.eqdCGpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("eqdipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqdIpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("eqdcpi", cnt -> InternalParetPostprocessProvider_intervalHist.eqdCIpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("eqdgpipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqdGpIpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("eqdcgpipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqdCGpIpi(cnt.slice, cnt.similarity, cnt.getEqd(), 20, 0.2));
			estProviders.put("wceeqd", cnt -> new WeightedCompositeEstimation(cnt.slice, cnt.getEqd(), IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER, 20, cnt.similarity));
			estProviders.put("wceeqdg", cnt -> new WeightedCompositeEstimation(cnt.slice, cnt.getEqd(), new CenterOfGravityRepresentativeProvider(cnt.getMcv()), 20, cnt.similarity));
			estProviders.put("eqn", cnt -> IntervalEstimation.eqn(cnt.slice, cnt.similarity, cnt.getEqn()));
			estProviders.put("eqnc", cnt -> GlobalCenterRepresentativeProvider.eqnC(cnt.slice, cnt.similarity, cnt.getEqn()));
			estProviders.put("heqnppc", cnt -> IntervalParetHybridEstimation.unknownConstant(cnt.slice, cnt.getEqn(), cnt.getMcv(), cnt.similarity));
			estProviders.put("eqngps", cnt -> WeighterdParetPostprocessProvider_sampledHist.eqnGps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqncgps", cnt -> WeighterdParetPostprocessProvider_sampledHist.eqnCGps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqnips", cnt -> InternalParetPostprocessProvider_sampledHist.eqnIps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqncips", cnt -> InternalParetPostprocessProvider_sampledHist.eqnCIps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqngpips", cnt -> InternalParetPostprocessProvider_sampledHist.eqnGpIps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqncgpips", cnt -> InternalParetPostprocessProvider_sampledHist.eqnCGpIps(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2, cnt.getSHist()));
			estProviders.put("eqngpi", cnt -> WeighterdParetPostprocessProvider_intervalHist.eqnGpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("eqncgpi", cnt -> WeighterdParetPostprocessProvider_intervalHist.eqnCGpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("eqnipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqnIpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("eqncpi", cnt -> InternalParetPostprocessProvider_intervalHist.eqnCIpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("eqngpipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqnGpIpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("eqncgpipi", cnt -> InternalParetPostprocessProvider_intervalHist.eqnCGpIpi(cnt.slice, cnt.similarity, cnt.getEqn(), 20, 0.2));
			estProviders.put("wceeqn", cnt -> new WeightedCompositeEstimation(cnt.slice, cnt.getEqn(), IntervalEstimation.DEFAULT_REPRESENTATIVE_PROVIDER, 20, cnt.similarity));
			estProviders.put("wceeqng", cnt -> new WeightedCompositeEstimation(cnt.slice, cnt.getEqn(), new CenterOfGravityRepresentativeProvider(cnt.getMcv()), 20, cnt.similarity));
		}
		return estProviders;
	}
	

}
