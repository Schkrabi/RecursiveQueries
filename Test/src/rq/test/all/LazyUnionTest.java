package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.SchemaNotEqualException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.latices.Lukasiewitz;
import rq.common.operators.LazyUnion;
import rq.common.operators.Union;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class LazyUnionTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	MemoryTable t1, t2, t3;
	LazyUnion u1, u2;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

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
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1),
						new Record.AttributeValuePair(b, "foo")), 
				0.7d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		
		t2 = new MemoryTable(this.schema);
		t2.insert(r1);
		t2.insert(r3);
		
		t3 = new MemoryTable(this.schema);
		t3.insert(r3);
		t3.insert(r4);
		
		u1 = LazyUnion.factory(t1, t2);
		u2 = LazyUnion.factory(t1, t3);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.Union#factory(rq.common.table.TabularExpression, rq.common.table.TabularExpression)}.
	 */
	@Test
	void testFactory() {
		assertThrows(
				SchemaNotEqualException.class,
				() -> Union.factory(
						t1, 
						new MemoryTable(Schema.factory(a)),
						Lukasiewitz.SUPREMUM));
	}

	/**
	 * Test method for {@link rq.common.operators.Union#eval()}.
	 */
	@Test
	void testEval() {
		Table rslt = LazyExpression.realizeInMemory(u1);
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertTrue(rcrds.contains(this.r3));
		
		rslt = LazyExpression.realizeInMemory(u2);
		rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertTrue(rcrds.contains(this.r3));
	}

	/**
	 * Test method for {@link rq.common.operators.Union#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.schema, u1.schema());
		assertEquals(this.schema, u2.schema());
	}

}
