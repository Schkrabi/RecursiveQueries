package rq.estimations.framework;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import rq.common.estimations.ConstantRepresentativeProvider;
import rq.common.estimations.IEstimation;
import rq.common.estimations.IntervalParetHybridEstimation;
import rq.common.estimations.ParPrecConst;
import rq.estimations.contracts.ConstantRestrictionExperimentContract;

/** Holds all estimations against constant against given query result*/
public class ConstantRestrictionExperiment {

	private ConstantRestrictionExperimentContract contract;
	
	private List<IEstimation> _estimations;
	
	public ConstantRestrictionExperiment(
			ConstantRestrictionExperimentContract contract) {
		this.contract = contract;
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
		return this.contract.getEstSignatures().stream()
				.map(sig -> {
					var c = estInitializer.get(sig);
					if(c == null) {
						//System.err.print("Estimation signature not recognized " + sig);
						return null;
					}
					return c.apply(this.contract);
				})
				.filter(est -> est != null)
				.toList();
	}
	
	private static Map<String, Function<ConstantRestrictionExperimentContract, IEstimation>> estInitializer =
			Map.of("ppc", cnt -> new ParPrecConst(
									cnt.getRsltInfo().queryInfo.attribute,
									cnt.getRsltInfo().slice, 
									cnt.getRsltInfo().queryInfo.constant, 
									cnt.getSimilarity(),
									//mcv.mostCommon(5)
									cnt.getMcv().mostCommon(20)),
					"eqdk", cnt -> ConstantRepresentativeProvider.eqdK(
									cnt.getRsltInfo().slice, 
									cnt.getSimilarity(), 
									cnt.getEqd(), 
									cnt.getRsltInfo().queryInfo.constant),
					"heqdppck", cnt -> IntervalParetHybridEstimation.knownConstant(
										cnt.getRsltInfo().slice, 
										cnt.getEqd(), 
										cnt.getMcv(), 
										cnt.getSimilarity(), 
										cnt.getRsltInfo().queryInfo.constant),
					"eqnk", cnt -> ConstantRepresentativeProvider.eqnK(
									cnt.getRsltInfo().slice, 
									cnt.getSimilarity(), 
									cnt.getEqn(), 
									cnt.getRsltInfo().queryInfo.constant),
					"heqnppck", cnt ->IntervalParetHybridEstimation.knownConstant(
										cnt.getRsltInfo().slice, 
										cnt.getEqn(), 
										cnt.getMcv(), 
										cnt.getSimilarity(), 
										cnt.getRsltInfo().queryInfo.constant)
					);
}
