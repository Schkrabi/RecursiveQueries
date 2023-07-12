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

import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.latices.Lukasiewitz;
import rq.common.operators.Join;
import rq.common.table.Record;
import rq.common.table.Attribute;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TypeSchemaMismatchException;

/**
 * @author r.skrabal
 *
 */
class JoinTest {
	
	Schema schema1, schema2;
	Attribute a, b, c;
	Record r11, r12, r21, r22;
	Table t1, t2;
	Join j1;

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
		this.c = new Attribute("C", String.class);
		schema1 = Schema.factory(a, b);
		schema2 = Schema.factory(a, c);
		r11 = Record.factory(
				schema1, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, "foo")),
				0.8d);
		r12 = Record.factory(
				schema1, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				0.7d);
		r21 = Record.factory(
				schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(c, "baz")),
				1.0d);
		r22 = Record.factory(
				schema2,
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(c, "bah")),
				0.4d);
		
		t1 = new Table(schema1);
		t1.insert(r11);
		t1.insert(r12);
		
		t2 = new Table(schema2);
		t2.insert(r21);
		t2.insert(r22);
		
		j1 = Join.factory(
				t1, 
				t2,
				Lukasiewitz.PRODUCT,
				new Join.AttributePair(new Attribute("A", Integer.class), new Attribute("A", Integer.class)));
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.Join#factory(rq.common.table.TabularExpression, rq.common.table.TabularExpression, java.util.Collection, java.util.function.BiFunction)}.
	 * @throws DuplicateAttributeNameException 
	 */
	@Test
	void testFactoryTabularExpressionTabularExpressionCollectionOfAttributePairBiFunctionOfDoubleDoubleDouble() throws DuplicateAttributeNameException {
		assertThrows(
				AttributeNotInSchemaException.class,
				() -> {
					Join.factory(
							t1, 
							t2,
							Arrays.asList(new Join.AttributePair(new Attribute("G", String.class), new Attribute("H", String.class))),
							Lukasiewitz.PRODUCT);
				});
		assertThrows(
				ComparisonDomainMismatchException.class,
				() -> {
					Join.factory(
							t1, 
							t2,
							Arrays.asList(new Join.AttributePair(new Attribute("A", Integer.class), new Attribute("C", String.class))),
							Lukasiewitz.PRODUCT);
				});
		Table t = new Table(Schema.factory(new Attribute("A", String.class), c));
		assertThrows(
				SchemaNotJoinableException.class,
				() -> {
					Join.factory(
							t1, 
							t,
							Arrays.asList(new Join.AttributePair(new Attribute("B", String.class), new Attribute("C", String.class))),
							Lukasiewitz.PRODUCT);
				});
	}

	/**
	 * Test method for {@link rq.common.operators.Join#eval()}.
	 * @throws DuplicateAttributeNameException 
	 * @throws AttributeNotInSchemaException 
	 * @throws TypeSchemaMismatchException 
	 */
	@Test
	void testEval() throws TypeSchemaMismatchException, AttributeNotInSchemaException, DuplicateAttributeNameException {
		Table rslt = j1.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(1, rcrds.size());
		assertTrue(rcrds.contains(
				Record.factory(Schema.factory(a, b, c), 
						Arrays.asList(
								new Record.AttributeValuePair(a, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d)));
		
	}

	/**
	 * Test method for {@link rq.common.operators.Join#schema()}.
	 * @throws DuplicateAttributeNameException 
	 */
	@Test
	void testSchema() throws DuplicateAttributeNameException {
		assertEquals(
				Schema.factory(a, b, c),
				j1.schema());
	}

}
