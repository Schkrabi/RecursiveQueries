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

	Schema schema, subschema, schema2;
	Attribute a, b, c, d;
	Record r1, r2, r3;
	Table t1;
	Projection p1, p2;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.c = new Attribute("C", Integer.class);
		this.schema = Schema.factory(a, b);
		
		this.subschema = Schema.factory(a);
		this.schema2 = Schema.factory(c, b);
		
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
		
		p1 = Projection.factory(
				t1, 
				subschema);
		p2 = Projection.factory(
				t1, 
				new Projection.To(a, c), 
				new Projection.To(b, b));
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
		
		rslt = this.p2.eval();
		rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(Record.factory(
				this.schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(c, 1), 
						new Record.AttributeValuePair(b, "foo")), 
				1.0d)));
		assertTrue(rcrds.contains(Record.factory(
				this.schema2,
				Arrays.asList(
						new Record.AttributeValuePair(c, 2), 
						new Record.AttributeValuePair(b,"bar")),  
				1.0d)));
		assertTrue(rcrds.contains(Record.factory(
				this.schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(c, 3), 
						new Record.AttributeValuePair(b,"foo")), 
				0.8d)));
	}

	/**
	 * Test method for {@link rq.common.operators.Projection#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.subschema, this.p1.schema());
		assertEquals(this.schema2, this.p2.schema());
	}

}
