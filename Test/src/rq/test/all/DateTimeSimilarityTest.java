package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;

import rq.common.similarities.DateTimeSimilarity;
import rq.common.types.DateTime;
import rq.files.helpers.JsonSerializer;

class DateTimeSimilarityTest {

	@Test
	void testApply() {
		var sf = new DateTimeSimilarity(3600);
		
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
		
		sf = new DateTimeSimilarity(Duration.ofDays(30).toSeconds());
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

	@Test
	void testSerialize() {
		var sf = new DateTimeSimilarity(3600);
		var json = JsonSerializer.instance().serialize(sf);
		var des = JsonSerializer.instance().deserialize(json, DateTimeSimilarity.class);
		assertEquals(sf, des);
	}
}
