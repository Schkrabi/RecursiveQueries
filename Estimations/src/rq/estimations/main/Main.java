package rq.estimations.main;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Main {

	public static void main(String[] args) throws Exception {
		//Just to remember default values
//		final int SLICES = 3;
//		final double DOMAIN_SAMPLE_SIZE = 0.05d;
//		final int STOCHASTIC_SAMPLES = 6;
//		final BiFunction<Object, Object, Double> SIMILARITY = LinearSimilarity.doubleSimilarityUntil(0.2);
//		final int probes = 10;
		
		//Usage:
		// java -jar <jarname> (contract file) (table) (table...)
		
//		var contract = EstimationSetupContract.factory(Path.of(
//				args[0]),
//				Arrays.asList(args).stream().skip(1).collect(Collectors.toList()));
//		
//		var provider = EstimationProviders.get(contract.getEstimation()).apply(contract);
//		
//		var measurement = new Measurement(provider);
//		
//		var rslt = 
//				measurement.measure();
//		
//		System.out.println(rslt);
	}
}
