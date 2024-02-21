package rq.estimations.main;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import rq.common.estimations.EstimateCrossJoin;
import rq.common.estimations.EstimateUnion;
import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.Numerical;
import rq.common.estimations.Numerical_domainPruning;
import rq.common.estimations.Numerical_stochastic;
import rq.common.estimations.Numerical_stochasticAndDomainPruning;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;
import rq.common.table.MemoryTable;

public class EstimationProviders {
	/** Numerical estimation */
	public static final Function<UnaryOperationContract, EstimationProvider> numerical = (contract) -> {
		var rslt = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addSampledHistogram(contract.getAttribute(), contract.getDomainSampleSize());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));

				Numerical est = new Numerical(selection, contract.getSlices(), contract.getDomainSampleSize());

				est.setProbes(contract.getProbes());
				return est.estimate();
			}

			@Override
			public String name() {
				return "numerical";
			}
		};
		return rslt;
	};

	/** Numerical estimation with domain pruning */
	public static final Function<UnaryOperationContract, EstimationProvider> numericalDomainPruning = (contract) -> {
		var ret = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addSampledHistogram(contract.getAttribute(), contract.getDomainSampleSize());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));

				Numerical est = new Numerical_domainPruning(selection, contract.getSlices(),
						contract.getDomainSampleSize());

				est.setProbes(contract.getProbes());
				return est.estimate();
			}

			@Override
			public String name() {
				return "numericalDomainPruning";
			}

		};
		return ret;
	};

	/** Numerical estimation with stochastic sampling */
	public static final Function<UnaryOperationContract, EstimationProvider> numericalStochastic = (contract) -> {
		var ret = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addSampledHistogram(contract.getAttribute(), contract.getDomainSampleSize());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));

				Numerical est = new Numerical_stochastic(selection, contract.getSlices(),
						contract.getDomainSampleSize(), contract.getStochasticSamples());

				est.setProbes(contract.getProbes());
				return est.estimate();
			}

			@Override
			public String name() {
				return "numericalStochastic";
			}
		};
		return ret;
	};

	/** Numerical estimation with domain pruning and stochastic sampling */
	public static final Function<UnaryOperationContract, EstimationProvider> numericalStochasticDomainPruning = (
			contract) -> {
		var ret = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addSampledHistogram(contract.getAttribute(), contract.getDomainSampleSize());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));
				Numerical est = new Numerical_stochasticAndDomainPruning(selection, contract.getSlices(),
						contract.getDomainSampleSize(), contract.getStochasticSamples());

				est.setProbes(contract.getProbes());
				return est.estimate();
			}

			@Override
			public String name() {
				return "numericalStochasticDomainPruning";
			}

		};
		return ret;
	};

	/** Interval estimation */
	public static final Function<UnaryOperationContract, EstimationProvider> interval_equidistant = (contract) -> {
		var ret = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addEquidistantHistogram(contract.getAttribute(), contract.getDomainSamples());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));

				IntervalEstimation est = new IntervalEstimation(selection, contract.getSlices(),
						contract.getDomainSamples());

				return est.estimate();
			}

			@Override
			public String name() {
				return "interval_equidistant";
			}

		};
		return ret;
	};

	/** Interval estimation */
	public static final Function<UnaryOperationContract, EstimationProvider> interval_equinominal = (contract) -> {
		var ret = new NumericDomainEstimationProvider(contract) {

			@Override
			public RankHistogram estimate() {
				var stats = contract.getTable().getStatistics();
				stats.addEquinominalHistogram(contract.getAttribute(), contract.getDomainSamples());
				stats.gather();

				var selection = new Selection(contract.getTable(), new rq.common.restrictions.Similar(
						contract.getAttribute(), new Constant<Double>(contract.getValue()), contract.getSimilarity()));

				IntervalEstimation est = new IntervalEstimation(selection, contract.getSlices(),
						contract.getDomainSamples());

				return est.estimate();
			}

			@Override
			public String name() {
				return "interval_equinominal";
			}

		};
		return ret;

	};

	/** Union estimation */
	public static final Function<BinaryOperationContract, EstimationProvider> union = (contract) -> {
		var ret = new EstimationProvider() {

			@Override
			public RankHistogram estimate() {
				var left = contract.getLeft().getStatistics();
				left.addRankHistogram(contract.getSlices());
				left.gather();

				var right = contract.getRight().getStatistics();
				right.addRankHistogram(contract.getSlices());
				right.gather();

				var rslt = EstimateUnion.estimate(left.getRankHistogram(contract.getSlices()).get(),
						right.getRankHistogram(contract.getSlices()).get());
				return rslt;
			}

			@Override
			public RankHistogram compute() {
				try {
					var union = rq.common.operators.Union.factory(
							contract.getLeft(), 
							contract.getRight(), 
							contract.getSupremum(), 
							(s, n) -> new MemoryTable(s));
					var rslt = union.eval();
					
					var stats = rslt.getStatistics();
					stats.addRankHistogram(contract.getSlices());
					stats.gather();
					var hist = stats.getRankHistogram(contract.getSlices()).get();
					return hist;
					
				} catch (SchemaNotEqualException e) {
					throw new RuntimeException(e);
				}
				
			}

			@Override
			public String name() {
				return "union";
			}

			@Override
			public EstimationSetupContract contract() {
				return contract;
			}

			@Override
			public int dataSize() {
				return contract.getLeft().size() + contract.getRight().size();
			}
			
		};
		return ret;
		
		
	};

	/** Crossjoin estimation */
	public static final Function<BinaryOperationContract, EstimationProvider> crossJoin = (contract) -> {
		var rslt = new EstimationProvider() {

			@Override
			public RankHistogram estimate() {
				var left = contract.getLeft().getStatistics();
				left.addRankHistogram(contract.getSlices());
				left.gather();

				var right = contract.getRight().getStatistics();
				right.addRankHistogram(contract.getSlices());
				right.gather();

				var rslt = EstimateCrossJoin.estimate(left.getRankHistogram(contract.getSlices()).get(),
						right.getRankHistogram(contract.getSlices()).get(), contract.getProduct());

				return rslt;
			}

			@Override
			public RankHistogram compute() {
				try {
					var crossjoin = rq.common.operators.Join.factory(
							contract.getLeft(), 
							contract.getRight(), 
							new rq.common.onOperators.OnEquals(new Constant<Boolean>(true), new Constant<Boolean>(true)));
					
					var rslt = crossjoin.eval();
					var stats = rslt.getStatistics();
					stats.addRankHistogram(contract.getSlices());
					stats.gather();
					var hist = stats.getRankHistogram(contract.getSlices()).get();
					
					return hist;
					
				} catch (OnOperatornNotApplicableToSchemaException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public String name() {
				return "crossJoin";
			}

			@Override
			public EstimationSetupContract contract() {
				return contract;
			}

			@Override
			public int dataSize() {
				return contract.getLeft().size() * contract.getRight().size();
			}
			
		};
		return rslt;		
	};
	
	private static Map<String, Function<? extends EstimationSetupContract, EstimationProvider>> list = 
			Map.of(
					"numerical", numerical,
					"numericalDomainPruning", numericalDomainPruning,
					"numericalStochastic", numericalStochastic,
					"numericalStochasticDomainPruning", numericalStochasticDomainPruning,
					"interval_equidistant", interval_equidistant,
					"interval_equinominal", interval_equinominal,
					"union", union,
					"crossJoin", crossJoin);
	/** Gets the estimation provider function based on its name */
	@SuppressWarnings("unchecked")
	public static Function<EstimationSetupContract, EstimationProvider> get(String name){
		return (Function<EstimationSetupContract, EstimationProvider>) list.get(name);
	}
	
	/** Gets name of all estimation providers */
	public Set<String> nameList(){
		return list.keySet();
	}
}
