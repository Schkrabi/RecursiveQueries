package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.InternalParetPostprocessProvider_sampledHist;
import rq.common.operators.Selection;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SlicedStatistic;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class ParetIntervalEstimation {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	Selection s1;
	IntervalEstimation e1;
	BiFunction<Object, Object, Double> similarity = rq.common.similarities.LinearSimilarity.doubleSimilarityUntil(10.0d);

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		//10x 1
		//5x 2
		
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", Double.class);
		this.schema = Schema.factory(a, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, 1.0d)),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b, 1.0d)), 
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b, 1.0d)), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 4), 
						new Record.AttributeValuePair(b, 1.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 5), 
						new Record.AttributeValuePair(b, 1.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 6), 
						new Record.AttributeValuePair(b, 3.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 7), 
						new Record.AttributeValuePair(b, 3.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 8), 
						new Record.AttributeValuePair(b, 3.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 9), 
						new Record.AttributeValuePair(b, 5.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 10), 
						new Record.AttributeValuePair(b, 5.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 11), 
						new Record.AttributeValuePair(b, 7.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 12), 
						new Record.AttributeValuePair(b, 7.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 13), 
						new Record.AttributeValuePair(b, 11.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 14), 
						new Record.AttributeValuePair(b, 13.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 15), 
						new Record.AttributeValuePair(b, 0.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 16), 
						new Record.AttributeValuePair(b, 17.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 17), 
						new Record.AttributeValuePair(b, 17.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 18), 
						new Record.AttributeValuePair(b, 19.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 19), 
						new Record.AttributeValuePair(b, 19.0d)), 
				0.8d));
		t1.insert(Record.factory(
				schema, 
				List.of(
						new Record.AttributeValuePair(a, 20), 
						new Record.AttributeValuePair(b, 23.0d)), 
				0.8d));
		
		t1.getStatistics().addSampledHistogram(b, 15.0);
		t1.getStatistics().addEquinominalHistogram(b, 2);
		t1.getStatistics().gather();
		
		e1 = InternalParetPostprocessProvider_sampledHist.eqnIps(
				3, 
				this.similarity, 
				t1.getStatistics().getEquinominalHistogram(b, 2).get(),
				1,
				0.80,
				t1.getStatistics().getSampledHistogram(b, 15.0d).get());
	}

	@Test
	void testEstimation() {
		var est = e1.estimate();
		assertNotNull(est);
		System.out.println(est);
	}

}
