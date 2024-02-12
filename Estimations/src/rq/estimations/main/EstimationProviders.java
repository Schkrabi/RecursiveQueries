package rq.estimations.main;

import java.util.List;
import java.util.function.BiFunction;

import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.Numerical;
import rq.common.estimations.Numerical_domainPruning;
import rq.common.estimations.Numerical_stochastic;
import rq.common.estimations.Numerical_stochasticAndDomainPruning;
import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;

public class EstimationProviders {
	
//	public static BiFunction<Selection, EstimationSetupContract, RankHistogram> parse(String estName){
//		List.of(EstimationProviders.class.getFields()).stream().filter(f -> )
//	}

	/** Numerical estimation */
	public static final BiFunction<Selection, EstimationSetupContract, RankHistogram> numerical =
			(	Selection selection,
				EstimationSetupContract contract) -> {
					Numerical est = new Numerical(
							selection,
							contract.getSlices(),
							contract.getDomainSampleSize());
					
				est.setProbes(contract.getProbes());
				return est.estimate();
			};
	
	/** Numerical estimation with domain pruning  */
	public static final BiFunction<Selection, EstimationSetupContract, RankHistogram> numericalDomainPruning =
			(	Selection selection,
					EstimationSetupContract contract) -> {
						Numerical est = new Numerical_domainPruning(
								selection,
								contract.getSlices(),
								contract.getDomainSampleSize());
						
					est.setProbes(contract.getProbes());
					return est.estimate();
				};
		
	/** Numerical estimation with stochastic sampling */
	public static final BiFunction<Selection, EstimationSetupContract, RankHistogram> numericalStochastic = 
			(	Selection selection,
					EstimationSetupContract contract) -> {
						Numerical est = new Numerical_stochastic(
								selection,
								contract.getSlices(),
								contract.getDomainSampleSize(),
								contract.getStochasticSamples());
						
					est.setProbes(contract.getProbes());
					return est.estimate();
				};
			
	/** Numerical estimation with domain pruning and stochastic sampling */
	public static final BiFunction<Selection, EstimationSetupContract, RankHistogram> numericalStochasticDomainPruning = 
			(	Selection selection,
					EstimationSetupContract contract) -> {
						Numerical est = new Numerical_stochasticAndDomainPruning(
								selection,
								contract.getSlices(),
								contract.getDomainSampleSize(),
								contract.getStochasticSamples());
						
					est.setProbes(contract.getProbes());
					return est.estimate();
				};
				
	/** Interval estimation */
	public static final BiFunction<Selection, EstimationSetupContract, RankHistogram> interval = 
			(	Selection selection,
					EstimationSetupContract contract) -> {
						IntervalEstimation est = new IntervalEstimation(
								selection,
								contract.getSlices(),
								contract.getDomainSamples());
						
					
					return est.estimate();
				};
}
