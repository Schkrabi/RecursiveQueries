/**
 * 
 */
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

import rq.common.operators.Projection;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.TypeSchemaMismatchException;

/**
 * @author r.skrabal
 *
 */
class ProjectionTest {

	Schema schema, subschema;
	Attribute a, b;
	Record r1, r2, r3;
	Table t1;
	Projection p1;
	
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
		
		this.subschema = Schema.factory(a);
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
		
		t1 = new Table(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		p1 = Projection.factory(t1, subschema);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.Projection#factory(rq.common.table.TabularExpression, rq.common.table.Schema)}.
	 */
	@Test
	void testFactory() {
		assertThrows(
				NotSubschemaException.class,
				() -> Projection.factory(
						t1, 
						Schema.factory(new Attribute("C", Integer.class)))
				);
	}

	/**
	 * Test method for {@link rq.common.operators.Projection#eval()}.
	 * @throws TypeSchemaMismatchException 
	 * @throws AttributeNotInSchemaException 
	 */
	@Test
	void testEval() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		Table rslt = this.p1.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(Record.factory(
				this.subschema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1)), 
				1.0d)));
		assertTrue(rcrds.contains(Record.factory(
				this.subschema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 2)),  
				1.0d)));
		assertTrue(rcrds.contains(Record.factory(
				this.subschema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3)), 
				0.8d)));
	}

	/**
	 * Test method for {@link rq.common.operators.Projection#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.subschema, this.p1.schema());
	}

}
