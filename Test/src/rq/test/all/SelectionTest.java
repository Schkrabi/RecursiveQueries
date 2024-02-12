/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.LesserThan;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

/**
 * 
 */
class SelectionTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	Selection s1;

	/**
	 * @throws java.lang.Exception
	 */
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
		
		s1 = new Selection(
				this.t1,
				new LesserThan(a, new Constant<Integer>(3)));
	}

	/**
	 * Test method for {@link rq.common.operators.Selection#eval()}.
	 */
	@Test
	void testEval() {
		Table rslt = this.s1.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(2, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertFalse(rcrds.contains(this.r3));
	}

	/**
	 * Test method for {@link rq.common.operators.Selection#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.t1.schema, s1.schema());
	}

}
