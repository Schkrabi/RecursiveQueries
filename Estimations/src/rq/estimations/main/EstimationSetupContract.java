package rq.estimations.main;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;

public class EstimationSetupContract {

	/** Number of slices for histogram */
	private int slices = 3;
	/** Number of probes for histogram if applicable */
	private int probes = 0;
	/** Number of samples for numerical domain if applicable */ 
	private int domainSamples = 100;
	/** Number of stochastic samples if applicable */
	private int stochasticSamples = 10;
	/** Is equinominal interval used? Otherwise use equidistant if applicable */
	private boolean equinominal = false;
	/** Domain sample size if applicable */
	private double domainSampleSize = 0.5d;
	
	public int getSlices() {
		return this.slices;
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
	
	public boolean getEquinominal() {
		return this.equinominal;
	}
	
	public double getDomainSampleSize() {
		return this.domainSampleSize;
	}
	
	public EstimationSetupContract(String path) {
		try {
			Scanner s = new Scanner(Path.of(path));
			this.deserialize(s);
			s.close();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void deserialize(Scanner s) {
		Map<String, String> args = new LinkedHashMap<String, String>();
		
		while(s.hasNext()) {
			String line = s.nextLine();
			String[] spl = line.split("=");
			
			args.put(spl[0], spl[1]);
		}		
		
		this.initFromMap(args);
	}
	
	protected void initFromMap(Map<String, String> args) {
		String slc = args.get("slices");
		if(slc != null) {
			this.slices = Integer.parseInt(slc);
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
		
		String eq = args.get("equinominal");
		if(eq !=  null) {
			this.equinominal = Boolean.parseBoolean(eq);
		}
		
		String dss = args.get("domainSampleSize");
		if(dss != null) {
			this.domainSampleSize = Double.parseDouble(dss);
		}
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("slices").append("=").append(this.getSlices()).append(";")
				.append("probes").append("=").append(this.getProbes()).append(";")
				.append("domainSamples").append("=").append(this.getDomainSamples()).append(";")
				.append("stochasticSamples").append("=").append(this.getStochasticSamples()).append(";")
				.append("equinominal").append("=").append(this.getEquinominal()).append(";")
				.append("domainSampleSize").append("=").append(this.getDomainSampleSize()).append(";")
				.toString();
	}
}
