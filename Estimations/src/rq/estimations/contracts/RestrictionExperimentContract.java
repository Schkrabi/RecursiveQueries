package rq.estimations.contracts;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import rq.common.table.Attribute;
import rq.files.contracts.QueryGenerationStrategy;

public class RestrictionExperimentContract {
	private Path dataPath;
	private int queryCount;
	private Collection<Attribute<Double>> attributes;
	private Collection<Integer> slices;
	
	private Map<Attribute<Double>, Collection<Integer>> intervals;
	private Map<Attribute<Double>, Double> similarUntil;
	private Random random;	
	
	private QueryGenerationStrategy queryGenerationStrategy;
	
	private String dataFileName;
	private Map<Attribute<Double>, rq.common.similarities.ISimilarity<Double>> similarity = new HashMap<>();
	private Path histFolder;
	private Path estFolder;
	
	private Map<Attribute<Double>, Collection<Integer>> consideredValues;
	private Map<Attribute<Double>, Collection<Double>> paretRatios;
	
	private boolean useRankedDataAsPrimary;
	
	private Map<Attribute<Double>, Collection<Double>> queryValues;

	public Path getDataPath() {
		return dataPath;
	}

	public void setDataPath(Path dataPath) {
		this.dataPath = dataPath;
	}

	public int getQueryCount() {
		return queryCount;
	}

	public void setQueryCount(int queryCount) {
		this.queryCount = queryCount;
	}

	public Collection<Attribute<Double>> getAttributes() {
		return attributes;
	}

	public void setAttributes(Collection<Attribute<Double>> attributes) {
		this.attributes = attributes;
	}

	public Collection<Integer> getSlices() {
		return slices;
	}

	public void setSlices(Collection<Integer> slices) {
		this.slices = slices;
	}

	public Map<Attribute<Double>, Collection<Integer>> getIntervals() {
		return intervals;
	}

	public void setIntervals(Map<Attribute<Double>, Collection<Integer>> intervals) {
		this.intervals = intervals;
	}

	public Map<Attribute<Double>, Double> getSimilarUntil() {
		return similarUntil;
	}

	public void setSimilarUntil(Map<Attribute<Double>, Double> similarUntil) {
		this.similarUntil = similarUntil;
	}

	public Random getRandom() {
		return random;
	}

	public void setRandom(Random random) {
		this.random = random;
	}

	public QueryGenerationStrategy getQueryGenerationStrategy() {
		return queryGenerationStrategy;
	}

	public void setQueryGenerationStrategy(QueryGenerationStrategy queryGenerationStrategy) {
		this.queryGenerationStrategy = queryGenerationStrategy;
	}

	public String getDataFileName() {
		return dataFileName;
	}

	public void setDataFileName(String dataFileName) {
		this.dataFileName = dataFileName;
	}

	public Map<Attribute<Double>, rq.common.similarities.ISimilarity<Double>> getSimilarity() {
		return similarity;
	}

	public void setSimilarity(Map<Attribute<Double>, rq.common.similarities.ISimilarity<Double>> similarity) {
		this.similarity = similarity;
	}

	public Path getHistFolder() {
		return histFolder;
	}

	public void setHistFolder(Path histFolder) {
		this.histFolder = histFolder;
	}

	public Path getEstFolder() {
		return estFolder;
	}

	public void setEstFolder(Path estFolder) {
		this.estFolder = estFolder;
	}

	public Map<Attribute<Double>, Collection<Integer>> getConsideredValues() {
		return consideredValues;
	}

	public void setConsideredValues(Map<Attribute<Double>, Collection<Integer>> consideredValues) {
		this.consideredValues = consideredValues;
	}

	public Map<Attribute<Double>, Collection<Double>> getParetRatios() {
		return paretRatios;
	}

	public void setParetRatios(Map<Attribute<Double>, Collection<Double>> paretRatios) {
		this.paretRatios = paretRatios;
	}

	public boolean isUseRankedDataAsPrimary() {
		return useRankedDataAsPrimary;
	}

	public void setUseRankedDataAsPrimary(boolean useRankedDataAsPrimary) {
		this.useRankedDataAsPrimary = useRankedDataAsPrimary;
	}

	public Map<Attribute<Double>, Collection<Double>> getQueryValues() {
		return queryValues;
	}

	public void setQueryValues(Map<Attribute<Double>, Collection<Double>> queryValues) {
		this.queryValues = queryValues;
	}
	
}
