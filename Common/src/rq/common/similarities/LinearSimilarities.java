package rq.common.similarities;

import rq.common.types.DateTime;

public class LinearSimilarities {

	public static final ISimilarity<Integer> integerSimilarityUntil(int similarUntil) {
		return new LinearIntSimilarity(similarUntil);
	}

	public static final ISimilarity<Double> doubleSimilarityUntil(double similarUntil) {
		return new LinearNumSimilarity(similarUntil);
	}

	public static final ISimilarity<DateTime> dateTimeSimilarityUntil(long l) {
		return new DateTimeSimilarity(l);		
	}

}
