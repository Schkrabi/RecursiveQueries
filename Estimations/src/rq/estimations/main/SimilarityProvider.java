package rq.estimations.main;

import java.util.Map;
import java.util.function.BiFunction;

import rq.common.similarities.LinearSimilarity;

public class SimilarityProvider {

	/** Singleton */
	private static SimilarityProvider instance = null;
	
	/** Name - Similarity map */
	private Map<String, BiFunction<Object, Object, Double>> similarities
	 	= Map.of(
	 			"dl0_5", LinearSimilarity.doubleSimilarityUntil(0.5d)
	 			);
	
	/** Singleton constructor */
	private SimilarityProvider() {}
	
	/** Gets the similarity by given name, or null */
	public BiFunction<Object, Object, Double> get(String name){
		return this.similarities.get(name);
	}
	
	/** Gets the similarity by given name, or null */
	public static BiFunction<Object, Object, Double> gets(String name){
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
