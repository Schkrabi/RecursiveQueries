package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.similarities.LinearSimilarity;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;

class ParetWeightedEstimation {

	//[0,50.0] = 50
	//[50.0,100.0] = 40
	//[100.0,150.0] = 10
	//consider 2 values (50.0; 100.0)
	//linear similarity to distance 100
	//Expected:
	//value: 50.0
	//	[0,50.0] similarity 1.0 -> [.66,1.0]
	//	[50.0,100.0] similarity 0.5 -> [.33,.66]
	//	[100.0,150.0] similarity 0 -> [.0,.33]
	//	weight: 50/100 = 0.5
	//value: 100.0
	//	[0,50.0] similarity 0.5 -> [.33,.66]
	//	[50.0,100.0] similarity 1.0 -> [.66,1.0]
	//	[100.0,150.0] similarity 0.5 -> [.33,.66]
	//	weight: 40/100 = 0.4
	//EXPECTED RESULT:
	//	[.0,.33] -> 10 * 0.5 = 5
	//	[.33,.66] -> 40 * 0.5 + 50 * 0.4 + 10 * 0.4 = 20 + 20 + 4 = 44
	//	[.66,1.0] -> 50 * 0.5 + 40 * 0.4 = 25 + 16 = 41
	
	rq.common.estimations.ParetWeightedEstimation_sampledHist estimation = null;
	SampledHistogram sampledHistogram = null;
	final BiFunction<Object, Object, Double> similarity = LinearSimilarity.doubleSimilarityUntil(100.0);
	final int numberOfConsideredValues = 2;
	final int resultSlices = 3;
	
	@BeforeEach
	void setUp() throws Exception {
		sampledHistogram = new SampledHistogram(
				new Attribute("A", Double.class),
				50.0d,
				Map.of(
						0.0d, 50,
						50.0d, 40,
						100.0d, 10));
		
		estimation = new rq.common.estimations.ParetWeightedEstimation_sampledHist(
				this.resultSlices,
				this.similarity,
				this.numberOfConsideredValues,
				this.sampledHistogram);
	}

	@Test
	void test() {
		var expected = new RankHistogram(this.resultSlices);
		expected.addIntervalValue(expected.fit(0), 5);
		expected.addIntervalValue(expected.fit(0.5d), 44);
		expected.addIntervalValue(expected.fit(1.0d), 41);

		RankHistogram est = null;
		est = this.estimation.estimate();
		assertNotNull(est);
		assertEquals(expected, est);
	}

}
