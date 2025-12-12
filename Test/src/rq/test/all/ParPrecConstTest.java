package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.estimations.ParPrecConst;
import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class ParPrecConstTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	Selection s1;
	BiFunction<Object, Object, Double> similarity = rq.common.similarities.LinearSimilarity.doubleSimilarityUntil(10);
	
	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", Double.class);
		this.schema = Schema.factory(a, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, 42d)),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b, 42d)), 
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b, 0d)), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		t1.getStatistics().addMostCommonValues(b);
		t1.getStatistics().gather();
	}

	@Test
	void test() {
		var est = new ParPrecConst(
				a, 
				3, 
				32, 
				this.similarity, 
				this.t1.getStatistics().getMostCommonValues(this.b).get().mostCommon(1));
		
		var hist = est.estimate();
		
		var rh = new RankHistogram(3);
		rh.addIntervalValue(rh.fit(this.similarity.apply(42d, 32d)), 2);
		
		assertEquals(rh, hist);
	}

}
