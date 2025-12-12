package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;

class HistBasedRandomTest {

	private Attribute A = new Attribute("A", Double.class);
	private Random rand;
	
	@BeforeEach
	void setUp() throws Exception {		
		rand = new Random(42);
	}

	@Test
	void testSampled() {
		var sHist = new SampledHistogram(A, 50.0d, Map.of(0.0d, 10, 50.0d, 30, 100.0d, 60));
		var hbRand = sHist.generator(this.rand);
		var d = hbRand.nextDouble();
		assertTrue(d > 100.0 && d < 150.0);
		d = hbRand.nextDouble();
		assertTrue(d > 50.0 && d < 100.0);
		d = hbRand.nextDouble();
		assertTrue(d > 100.0 && d < 150.0);
	}

	@Test
	void testDataSliced() {
		var rnks = IntStream.iterate(0, i -> i+1).limit(100)
				.mapToDouble(i -> i < 10 ? 1d : i < 40 ? 51d : 101d)
				.boxed().collect(Collectors.toList());
		var eqd = DataSlicedHistogram.create(
				new DataSlicedHistogram.Interval[]{
						new DataSlicedHistogram.Interval(0, 50, false, true), 
						new DataSlicedHistogram.Interval(50, 100, false, true), 
						new DataSlicedHistogram.Interval(100, 150, false, true)}, 
				rnks, 
				A, 
				3);
		var hbRand = eqd.generator(this.rand);
		var d = hbRand.nextDouble();
		assertTrue(d > 100.0 && d < 150.0);
		d = hbRand.nextDouble();
		assertTrue(d > 50.0 && d < 100.0);
		d = hbRand.nextDouble();
		assertTrue(d > 100.0 && d < 150.0);
	}
}
