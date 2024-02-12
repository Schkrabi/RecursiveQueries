package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.AttributeHistogram;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class AttributeHistogramTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	
	AttributeHistogram vc;

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
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,"foo")), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
	}

	@Test
	void testGather() {
		this.vc = new AttributeHistogram(this.b);
		
		assertEquals(0, this.vc.getCount("foo"));
		assertEquals(0, this.vc.getCount("bar"));
		assertEquals(0, this.vc.getCount("baz"));
		assertTrue(this.vc.getHistogram().isEmpty());
		
		this.vc.gather(this.t1);
		
		assertEquals(2, this.vc.getCount("foo"));
		assertEquals(1, this.vc.getCount("bar"));
		assertEquals(0, this.vc.getCount("baz"));
		assertFalse(this.vc.getHistogram().isEmpty());
	}

}
