package rq.estimations.main;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public abstract class EstimationSetupContract {

	/** Number of slices for histogram */
	private int slices = 3;
	
	private String estimation;
	
	public int getSlices() {
		return this.slices;
	}
	
	public String getEstimation() {
		return this.estimation;
	}
	
	public abstract void setTables(List<String> paths);
	
	/** Sets the table arguments */
	public void setTables(String ...paths) {
		this.setTables(Arrays.asList(paths));
	}	
	
	protected EstimationSetupContract() {}

	public static Map<String, String> deserialize(Scanner s) {
		Map<String, String> args = new LinkedHashMap<String, String>();
		
		while(s.hasNext()) {
			String line = s.nextLine();
			String[] spl = line.split("=");
			
			args.put(spl[0], spl[1]);
		}		
		
		return args;
	}
	
	protected void initFromMap(Map<String, String> args) {
		String slc = args.get("slices");
		if(slc != null) {
			this.slices = Integer.parseInt(slc);
		}
		
		var est = args.get("estimation");
		if(est != null) {
			this.estimation = est;
		}
		else {
			throw new RuntimeException("Estimation must be provided.");
		}
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("slices").append("=").append(this.getSlices()).append(";")
				.toString();
	}
	
	public static EstimationSetupContract factory(Path path, List<String> tables) {
		Map<String, String> argMap = null;
		try {
			argMap = deserialize(new Scanner(path));
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
		
		EstimationSetupContract contract = null;
		
		switch(argMap.get("estimation")) {
		case "numerical":
		case "numericalDomainPruning":
		case "numericalStochastic":
		case "numericalStochasticDomainPruning":
		case "interval_equidistant":
		case "interval_equinominal":
			contract = new UnaryOperationContract();
			break;
		case "union":
		case "crossJoin":
			contract = new BinaryOperationContract();
			break;
		default:
			throw new RuntimeException("Estimation not recognized.");
		}
		
		contract.setTables(tables);
		contract.initFromMap(argMap);
		return contract;
	}
}
