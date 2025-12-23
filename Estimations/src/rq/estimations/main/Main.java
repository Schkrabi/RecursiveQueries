package rq.estimations.main;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;

import rq.files.contracts.EstimationExperimentContract;

public class Main {

	public static void main(String[] args) throws Exception {
		if(args.length == 0) {
			System.out.println("Usage est.jar <conf file>");
			return;
		}
		var confName = args[0];
		var confJson = Files.readString(Path.of(confName));
		var cnt = EstimationExperimentContract.deserialize(confJson);
		
		var experiment = new ParametrizedExperiment(cnt);
		
		var start = System.currentTimeMillis();
		experiment.experiment();
		var end = System.currentTimeMillis();
		
		System.out.println("Finished, time: " + Duration.ofMillis(end - start).toString());
	}
}
