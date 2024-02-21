package rq.estimations.main;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import rq.common.interfaces.Table;
import rq.common.table.Attribute;
import rq.files.io.TableReader;

public class UnaryOperationContract extends EstimationSetupContract {

	private Table table;
	private Attribute attribute;
	private BiFunction<Object, Object, Double> similarity;
	private String similarityName;
	private double value;
	/** Number of probes for histogram if applicable */
	private int probes = 0;
	/** Number of samples for numerical domain if applicable */ 
	private int domainSamples = 100;
	/** Number of stochastic samples if applicable */
	private int stochasticSamples = 10;
	/** Domain sample size if applicable */
	private double domainSampleSize = 0.5d;
	
	public Attribute getAttribute() {
		return this.attribute;
	}
	
	public BiFunction<Object, Object, Double> getSimilarity(){
		return this.similarity;
	}
	
	public double getValue() {
		return this.value;
	}
	
	public int getProbes() {
		return this.probes;
	}
	
	public int getDomainSamples() {
		return this.domainSamples;
	}
	
	public int getStochasticSamples() {
		return this.stochasticSamples;
	}
	
	public double getDomainSampleSize() {
		return this.domainSampleSize;
	}
	
	public Table getTable() {
		return this.table;
	}
	
	public UnaryOperationContract() {
		super();
	}
	
	@Override
	protected void initFromMap(Map<String, String> args) {
		super.initFromMap(args);
		
		var att = args.get("attribute");
		if(att != null) {
			this.attribute = this.table.schema().attributeByName(att);
		}
		else {
			throw new RuntimeException("Attribute must be specified.");
		}
		
		var sim = args.get("similarity");
		if(sim != null) {
			this.similarity = SimilarityProvider.gets(sim);
			this.similarityName = sim;
		}
		else {
			throw new RuntimeException("Simialrity must be specified.");
		}
		
		var val = args.get("value");
		if(val != null) {
			this.value = Double.parseDouble(val);
		}
		else {
			throw new RuntimeException("Value must be specified.");
		}
		
		String pro = args.get("probes");
		if(pro != null) {
			this.probes = Integer.parseInt(pro);
		}
		String ds = args.get("domainSamples");
		if(ds != null) {
			this.domainSamples = Integer.parseInt(ds);
		}
		
		String stoch = args.get("stochasticSamples");
		if(stoch != null) {
			this.stochasticSamples = Integer.parseInt(stoch);
		}
		
		String dss = args.get("domainSampleSize");
		if(dss != null) {
			this.domainSampleSize = Double.parseDouble(dss);
		}
	}

	@Override
	public String toString() {
		return new StringBuilder(super.toString())
				.append("attribute").append("=").append(this.attribute.name).append(";")
				.append("similarity").append("=").append(this.similarityName).append(";")
				.append("probes").append("=").append(this.probes).append(";")
				.append("probes").append("=").append(this.getProbes()).append(";")
				.append("domainSamples").append("=").append(this.getDomainSamples()).append(";")
				.append("stochasticSamples").append("=").append(this.getStochasticSamples()).append(";")
				.append("domainSampleSize").append("=").append(this.getDomainSampleSize()).append(";")
				.toString();
	}

	@Override
	public void setTables(List<String> paths) {
		try {
		var tableReader = TableReader.open(Path.of(paths.get(0)));
		
		this.table = tableReader.read();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
}
