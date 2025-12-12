package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.DataSlicedHistogram;
import rq.common.statistic.EquidistantHistogram;
import rq.common.table.Attribute;

class DataSlicedHistogramTest {

	private Attribute A = new Attribute("A", Double.class);
	
	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testSerialization() {
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
		
		var ser = eqd.serialize();
		EquidistantHistogram des;
		try {
			des = EquidistantHistogram.deserialize(ser);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		assertEquals(eqd, des);
	}

}
