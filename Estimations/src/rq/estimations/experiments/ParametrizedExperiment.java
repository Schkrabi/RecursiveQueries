package rq.estimations.experiments;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Attribute;
import rq.estimations.framework.Experiment;
import rq.files.contracts.EstimationExperimentContract;
import rq.files.contracts.QueryGenerationStrategy;

public class ParametrizedExperiment extends Experiment {
	
	private final EstimationExperimentContract contract;
	
	public ParametrizedExperiment(EstimationExperimentContract cnt) {
		this.contract = cnt;
	}

	@Override
	protected Path folder() {
		return Path.of(this.contract.workingDirectory);
	}

	@Override
	protected String primaryDataFileName() {
		return this.contract.file;
	}

	@Override
	protected String preparedDataName() {
		return "prepared";
	}

	
	private Map<Attribute<Double>, Collection<Integer>> cnsdrvls = null;
	
	@Override
	protected Map<Attribute<Double>, Collection<Integer>> nConsideredValues() {
		if(cnsdrvls == null) {
			cnsdrvls = new HashMap<>();
			this.contract.attributes.stream()
				.filter(ac -> ac.isNumericAttribute())
				.forEach(ac -> cnsdrvls.put(ac.getDoubleAttribute(), List.of(ac.consideredValues)));
		}
		return cnsdrvls;
	}

	private Map<Attribute<Double>, Collection<Double>> prtrat = null;
	
	@Override
	protected Map<Attribute<Double>, Collection<Double>> paretRatios() {
		if(prtrat == null) {
			prtrat = new HashMap<>();
			this.contract.attributes.stream()
				.filter(ac -> ac.isNumericAttribute())
				.forEach(ac -> prtrat.put(ac.getDoubleAttribute(), List.of(ac.paretValue)));
		}
		return prtrat;
	}

	@Override
	protected TabularExpression prepareDataQuery(Table primaryData) {
		//TODO
		return primaryData;
	}

	@Override
	protected List<Integer> slices() {
		return List.of(this.contract.slices);
	}

	List<Attribute<Double>> atts = null;
	
	@Override
	protected List<Attribute<Double>> numericAttributes() {
		if(atts == null) {
			atts = this.contract.attributes.stream()
					.filter(ac -> ac.isNumericAttribute())
					.map(a -> a.getDoubleAttribute()).toList();
		}
		return atts;
	}

	@Override
	protected List<Attribute<?>> nominalAttributes() {
		//TODO
		return List.of();
	}

	private Map<Attribute<Double>, Double> hss = null;
	
	@Override
	protected double histSampleSize(Attribute<Double> a) {
		if(hss == null) {
			hss = new HashMap<>();
			this.contract.attributes.stream()
				.filter(ac -> ac.isNumericAttribute())
				.forEach(ac -> hss.put(ac.getDoubleAttribute(), ac.histSampleSize));
		}
		return hss.get(a);
	}

	Map<Attribute<?>, List<Integer>> intvs = null;
	
	@Override
	protected List<Integer> intervals(Attribute<?> a) {
		if(intvs == null) {
			intvs = new HashMap<>();
			for(var at : this.contract.attributes) {
				intvs.put(at.getAttribute(), List.of(at.intervals));
			}
		}
		return intvs.get(a);
	}

	Map<Attribute<Double>, Double> sml = null;
	
	@Override
	protected double similarUntil(Attribute<Double> a) {
		if(sml == null) {
			sml = new HashMap<>();
			this.contract.attributes.stream()
				.filter(ac -> ac.isNumericAttribute())
				.forEach(ac -> sml.put(ac.getDoubleAttribute(), ac.similarUntil));
		}
		return sml.get(a);
	}
	
	@Override
	protected List<Integer> probes() {
		return List.of(this.contract.probes);
	}

	@Override
	protected List<Integer> estSamples(Attribute<?> a) {
		return List.of();
	}
	
	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		//TODO
		return Map.of();
	}

	@Override
	protected Map<String, List<Attribute<?>>> smallData() {
		//TODO
		return Map.of();
	}

	@Override
	protected List<Attribute<?>> projectionAttributes() {
		// TODO Auto-generated method stub
		return List.of();
	}

	@Override
	protected long seed() {
		return this.contract.seed;
	}

	private Map<Attribute<Double>, Collection<Double>> queryValues = null;
	@Override
	protected Map<Attribute<Double>, Collection<Double>> getQueryValues(){
		if(this.queryValues == null) {
			this.queryValues = new HashMap<>();
			this.contract.attributes.stream()
				.filter(ac -> ac.isNumericAttribute())
				.forEach(ac -> this.queryValues.put(ac.getDoubleAttribute(), new ArrayList<>(ac.restrictionQryArgs)));
		}
		return this.queryValues;
	}
	
	@Override
	protected QueryGenerationStrategy getQueryGenerationStrategy() {
		return this.contract.getQueryGenerationStrategy();
	}
}
