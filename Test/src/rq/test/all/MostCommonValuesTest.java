package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.statistic.AttributeHistogram;
import rq.common.statistic.MostCommonValues;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.util.Pair;

class MostCommonValuesTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	MostCommonValues mcv;

	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", Double.class);
		this.schema = Schema.factory(a, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, 0.1)),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,0.1)), 
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,0.2)), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		this.t1.statistics.addMostCommonValues(b);
		this.t1.statistics.gather();
		var omcv = this.t1.statistics.getMostCommonValues(b);
		if(omcv.isEmpty()) {
			fail("MCV not found");
		}
		this.mcv = omcv.get();
	}

	@Test
	void test() {
		assertEquals(List.of(Pair.of(0.1, 2)), mcv.mostCommon(1));
		assertEquals(List.of(Pair.of(0.1, 2), Pair.of(0.2,  1)), mcv.mostCommon(2));
		assertEquals(List.of(Pair.of(0.1, 2), Pair.of(0.2, 1)), mcv.mostCommon(100));
	}
	
	@Test
	void testIO() {
		var ser = this.mcv.serialize();
		var des = MostCommonValues.deserialize(ser);
		assertEquals(mcv, des);
	}

}
