package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.SlicedHistogram;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class SlicedHistogramTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4, r5, r6;
	MemoryTable t1;

	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, "foo")),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				0.95d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,"foo")), 
				0.6d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 4), 
						new Record.AttributeValuePair(b,"foo")), 
				0.45d);
		r5 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 5), 
						new Record.AttributeValuePair(b,"bar")), 
				0.2d);
		r6 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 6), 
						new Record.AttributeValuePair(b,"foo")), 
				0.15d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		t1.insert(r4);
		t1.insert(r5);
		t1.insert(r6);
	}

	@Test
	void testGather() {
		SlicedHistogram sh = new SlicedHistogram(b, 3);
		
		Set<SlicedHistogram.RankInterval> slices = sh.getSlices(); 
		assertEquals(3, slices.size());
		
		assertAll(() -> {
			sh.gather(t1);
		});
		
		SlicedHistogram.RankInterval i1 = slices.stream().filter(i -> i.start == 0.0d).findAny().get();
		SlicedHistogram.RankInterval i2 = slices.stream().filter(i -> i.start != 0.0d && i.end != 1.0d).findAny().get();
		SlicedHistogram.RankInterval i3 = slices.stream().filter(i -> i.end == 1.0d).findAny().get();
		
		assertEquals(1, sh.getCount(i1.start, i1.end, "foo"));
		assertEquals(1, sh.getCount(i1.start, i1.end, "bar"));
		
		assertEquals(2, sh.getCount(i2.start, i2.end, "foo"));
		assertEquals(0, sh.getCount(i2.start, i2.end, "bar"));
		
		assertEquals(1, sh.getCount(i3.start, i3.end, "foo"));
		assertEquals(1, sh.getCount(i3.start, i3.end, "bar"));
		
		assertEquals(0, sh.getCount(0.0d,  1.0d, "foo"));
		assertEquals(0, sh.getCount(i1, "baz"));
		
		assertEquals(2, sh.sliceSize(i1));
		assertEquals(2, sh.sliceSize(i2));
		assertEquals(2, sh.sliceSize(i3));
		assertEquals(0, sh.sliceSize(0.0d, 1.0d));
	}
}
