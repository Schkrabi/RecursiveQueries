package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

import org.junit.jupiter.api.Test;

import rq.common.similarities.NominalSimilarity;
import rq.common.util.Pair;
import rq.files.helpers.JsonSerializer;

class NominalSimilarityTest {
	
	NominalSimilarity<String> ns = new NominalSimilarity<String>(Map.of(Pair.of("foo", "bar"), 0.5d,
			Pair.of("foo", "baz"), 0.25d));
		
	@Test
	void testApply() {
		assertEquals(0.5d, ns.apply("foo", "bar"));
		assertEquals(0.0d, ns.apply("bar", "baz"));
		assertEquals(0.25d, ns.apply("baz", "foo"));
		assertEquals(1.0d, ns.apply("foo", "foo"));
	}

	@Test
	void testSerialize() {
		var json = JsonSerializer.instance().serialize(ns);
		var res = JsonSerializer.instance().deserialize(json, NominalSimilarity.class);
		assertEquals(ns, res);
	}
}
