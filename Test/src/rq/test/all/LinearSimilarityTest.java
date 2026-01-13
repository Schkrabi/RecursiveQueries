package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.similarities.LinearSimilarities;
import rq.common.types.DateTime;

class LinearSimilarityTest {

	@BeforeEach
	void setUp() throws Exception {
	}

	@Test
	void testIntegerSimilarityUntil() {
		var sf = LinearSimilarities.integerSimilarityUntil(10);
		
		double s = sf.apply(0, 0);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 1.0d);
		
		s = sf.apply(0, 10);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
		
		s = sf.apply(0,  3);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(0, 999);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
	}

	@Test
	void testDoubleSimilarityUntil() {
		var sf = LinearSimilarities.doubleSimilarityUntil(2.0d);
		
		double s = sf.apply(0.0d, 0.0d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 1.0d);
		
		s = sf.apply(0.0d, 2.0d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
		
		s = sf.apply(0.0d, 0.5d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(0.0d, 999.0d);
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
	}

	@Test
	void testDateTimeSimilarityUntil() {
		var sf = LinearSimilarities.dateTimeSimilarityUntil(3600);
		
		double s = sf.apply(
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 0, 0)),
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 0, 0)),
				new DateTime(LocalDateTime.of(2023, 9, 28, 7, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 0, 0)),
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 15, 30)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2023, 9, 28, 6, 0, 0)),
				new DateTime(LocalDateTime.of(2023, 12, 31, 23, 59, 59)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
		
		sf = LinearSimilarities.dateTimeSimilarityUntil(Duration.ofDays(30).toSeconds());
		s = sf.apply(
				new DateTime(LocalDateTime.of(2010, 12, 30, 0, 0, 0)),
				new DateTime(LocalDateTime.of(2011, 1, 6, 0, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2010, 12, 30, 0, 0, 0)),
				new DateTime(LocalDateTime.of(2011, 1, 13, 0, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2010, 12, 30, 0, 0, 0)),
				new DateTime(LocalDateTime.of(2011, 1, 20, 0, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2010, 12, 30, 0, 0, 0)),
				new DateTime(LocalDateTime.of(2011, 1, 27, 0, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		
		s = sf.apply(
				new DateTime(LocalDateTime.of(2010, 12, 30, 0, 0, 0)),
				new DateTime(LocalDateTime.of(2011, 2, 3, 0, 0, 0)));
		assertTrue(s >= 0.0d);
		assertTrue(s <= 1.0d);
		assertEquals(s, 0.0d);
	}

}
