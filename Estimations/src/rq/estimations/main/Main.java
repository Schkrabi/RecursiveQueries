package rq.estimations.main;

import java.nio.file.Path;

import rq.common.interfaces.Table;
import rq.files.io.TableReader;

public class Main {

	public static void main(String[] args) throws Exception {
		//Just to remember default values
//		final int SLICES = 3;
//		final double DOMAIN_SAMPLE_SIZE = 0.05d;
//		final int STOCHASTIC_SAMPLES = 6;
//		final BiFunction<Object, Object, Double> SIMILARITY = LinearSimilarity.doubleSimilarityUntil(0.2);
//		final int probes = 10;
		
		//Usage:
		// java -jar <jarname> (table file) (queried attribute) (queried value) (similarity) (estimate algorithm) (estimate setup file)
		
		var tableFile = Path.of(args[0]);
		var atributeName = args[1];
		var value = Double.parseDouble(args[2]);
		var similarityName = args[3];
		var estimateProvider = EstimationProviders.parse(args[4]);
		var contract = new EstimationSetupContract(args[5]);
		
		//Load data
		var reader = TableReader.open(tableFile);
		Table data = reader.read();
		
		var attribute = data.schema().attributeByName(atributeName);
		
		var measurement =
				new Measurement(
						attribute, 
						value, 
						data, 
						contract, 
						similarityName, 
						estimateProvider);
		
		var rslt = 
				measurement.measure();
		
		System.out.println(rslt);
	}
}
