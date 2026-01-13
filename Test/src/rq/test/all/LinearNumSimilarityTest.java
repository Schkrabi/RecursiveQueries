package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import rq.common.similarities.LinearNumSimilarity;
import rq.files.helpers.JsonSerializer;

class LinearNumSimilarityTest {

	@Test
	void testApply() {
		var sf = new LinearNumSimilarity(10);
		
		double s = sf.apply(0d, 0d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 1.0d);
		
		s = sf.apply(0d, 10d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
		
		s = sf.apply(0d,  3d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(0d, 999d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
	}

	@Test
	void testSerialize() {
		var sf = new LinearNumSimilarity(10);
		var json = JsonSerializer.instance().serialize(sf);
		var des = JsonSerializer.instance().deserialize(json, LinearNumSimilarity.class);
		assertEquals(sf, des);
	}

}
