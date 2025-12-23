package rq.estimations.main;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.table.Attribute;
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

	
	private Map<Attribute, Collection<Integer>> cnsdrvls = null;
	
	@Override
	protected Map<Attribute, Collection<Integer>> nConsideredValues() {
		if(cnsdrvls == null) {
			cnsdrvls = new HashMap<Attribute, Collection<Integer>>();
			for(var a : this.contract.attributes) {
				cnsdrvls.put(a.getAttribute(), List.of(a.consideredValues));
			}
		}
		return cnsdrvls;
	}

	private Map<Attribute, Collection<Double>> prtrat = null;
	
	@Override
	protected Map<Attribute, Collection<Double>> paretRatios() {
		if(prtrat == null) {
			prtrat = new HashMap<Attribute, Collection<Double>>();
			for(var a : this.contract.attributes) {
				prtrat.put(a.getAttribute(), List.of(a.paretValue));
			}
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

	List<Attribute> atts = null;
	
	@Override
	protected List<Attribute> numericAttributes() {
		if(atts == null) {
			atts = this.contract.attributes.stream().map(a -> a.getAttribute()).toList();
		}
		return atts;
	}

	@Override
	protected List<Attribute> nominalAttributes() {
		//TODO
		return List.of();
	}

	private Map<Attribute, Double> hss = null;
	
	@Override
	protected double histSampleSize(Attribute a) {
		if(hss == null) {
			hss = new HashMap<>();
			for(var at : this.contract.attributes) {
				hss.put(at.getAttribute(), at.histSampleSize);
			}
		}
		return hss.get(a);
	}

	Map<Attribute, List<Integer>> intvs = null;
	
	@Override
	protected List<Integer> intervals(Attribute a) {
		if(intvs == null) {
			intvs = new HashMap<>();
			for(var at : this.contract.attributes) {
				intvs.put(at.getAttribute(), List.of(at.intervals));
			}
		}
		return intvs.get(a);
	}

	Map<Attribute, Double> sml = null;
	
	@Override
	protected double similarUntil(Attribute a) {
		if(sml == null) {
			sml = new HashMap<>();
			for(var at : this.contract.attributes) {
				sml.put(at.getAttribute(), at.similarUntil);
			}
		}
		return sml.get(a);
	}
	
	@Override
	protected List<Integer> probes() {
		return List.of(this.contract.probes);
	}

	@Override
	protected List<Integer> estSamples(Attribute a) {
		return List.of();
	}
	
	@Override
	protected Map<String, TabularExpression> prepareSubDataQueries(Table preparedData) {
		//TODO
		return Map.of();
	}

	@Override
	protected Map<String, List<Attribute>> smallData() {
		//TODO
		return Map.of();
	}

	@Override
	protected List<Attribute> projectionAttributes() {
		// TODO Auto-generated method stub
		return List.of();
	}

	@Override
	protected long seed() {
		return this.contract.seed;
	}

	private Map<Attribute, Collection<Double>> queryValues = null;
	@Override
	protected Map<Attribute, Collection<Double>> getQueryValues(){
		if(this.queryValues == null) {
			this.queryValues = new HashMap<>();
			for(var at : this.contract.attributes) {
				this.queryValues.put(at.getAttribute(), new ArrayList<>(at.restrictionQryArgs));
			}
		}
		return this.queryValues;
	}
	
	@Override
	protected QueryGenerationStrategy getQueryGenerationStrategy() {
		return this.contract.getQueryGenerationStrategy();
	}
}
