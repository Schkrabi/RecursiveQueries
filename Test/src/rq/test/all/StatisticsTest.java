package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.Statistics;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class StatisticsTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	
	Statistics s1;
	
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
		
		s1 = new Statistics(this.t1);
	}

	@Test
	void testGather() {
		this.s1.addAttributeHistogram(b);
		
		assertAll(() -> {
			this.s1.gather();
		});
	}

	@Test
	void testGetAllAndAddValueCount() {
		assertTrue(this.s1.getAll().isEmpty());
		assertAll(() -> {
			this.s1.addAttributeHistogram(b);
		});
		assertFalse(this.s1.getAll().isEmpty());
	}

	@Test
	void testGetValueCount() {
		this.s1.addAttributeHistogram(b);
		this.s1.gather();
		assertTrue(this.s1.getAttributeHistogram(b).isPresent());
		assertTrue(this.s1.getAttributeHistogram(a).isEmpty());
	}

}
