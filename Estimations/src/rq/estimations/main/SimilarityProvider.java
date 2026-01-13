package rq.estimations.main;

import java.util.Map;

import rq.common.similarities.LinearSimilarities;

public class SimilarityProvider {

	/** Singleton */
	private static SimilarityProvider instance = null;
	
	/** Name - Similarity map */
	private Map<String, rq.common.similarities.ISimilarity<Double>> similarities
	 	= Map.of(
	 			"dl0_5", LinearSimilarities.doubleSimilarityUntil(0.5d)
	 			);
	
	/** Singleton constructor */
	private SimilarityProvider() {}
	
	/** Gets the similarity by given name, or null */
	public rq.common.similarities.ISimilarity<Double> get(String name){
		return this.similarities.get(name);
	}
	
	/** Gets the similarity by given name, or null */
	public static rq.common.similarities.ISimilarity<Double> gets(String name){
		return instance().get(name);
	}

	/** singleton instance provider */
	public static SimilarityProvider instance() {
		if(instance == null) {
			instance = new SimilarityProvider();
		}
		return instance;
	}
}
