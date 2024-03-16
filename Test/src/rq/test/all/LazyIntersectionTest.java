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
import rq.common.operators.Intersection;
import rq.common.operators.LazyIntersection;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class LazyIntersectionTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	MemoryTable t1, t2, t3;
	LazyIntersection i1, i2;

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
		
		i1 = LazyIntersection.factory(t1, t2);
		i2 = LazyIntersection.factory(t1, t3);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.Intersection#factory(rq.common.table.TabularExpression, rq.common.table.TabularExpression)}.
	 */
	@Test
	void testFactory() {
		assertThrows(
				SchemaNotEqualException.class,
				() -> Intersection.factory(
						t1, 
						new MemoryTable(Schema.factory(a)),
						Lukasiewitz.INFIMUM)
				);
	}

	/**
	 * Test method for {@link rq.common.operators.Intersection#eval()}.
	 */
	@Test
	void testEval() {
		Table rslt = LazyExpression.realizeInMemory(i1);
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(1, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		
		rslt = LazyExpression.realizeInMemory(i2);
		rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(1, rcrds.size());
		assertTrue(rcrds.contains(this.r4));
	}

	/**
	 * Test method for {@link rq.common.operators.Intersection#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.schema, i1.schema());
		assertEquals(this.schema, i2.schema());
	}

}
