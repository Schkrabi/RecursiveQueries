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

import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.latices.Lukasiewitz;
import rq.common.operators.Join;
import rq.common.similarities.NaiveSimilarity;
import rq.common.table.Record;
import rq.common.table.Attribute;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.onOperators.OnGreaterThanOrEquals;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TypeSchemaMismatchException;

import rq.common.interfaces.Table;

/**
 * @author r.skrabal
 *
 */
class JoinTest {
	
	Schema schema1, schema2, expected;
	Attribute a, b, c, la, ra;
	Record r11, r12, r21, r22;
	MemoryTable t1, t2;
	Join j1, j2, j3;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.c = new Attribute("C", String.class);
		this.la = new Attribute("left." + a.name, a.domain);
		this.ra = new Attribute("right." + a.name, a.domain);
		schema1 = Schema.factory(a, b);
		schema2 = Schema.factory(a, c);
		expected = Schema.factory(
					la,
					ra,
					b, 
					c);
		
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
		
		t1 = new MemoryTable(schema1);
		t1.insert(r11);
		t1.insert(r12);
		
		t2 = new MemoryTable(schema2);
		t2.insert(r21);
		t2.insert(r22);
		
		j1 = Join.factory(
				t1, 
				t2,
				Lukasiewitz.PRODUCT,
				Lukasiewitz.INFIMUM,
				new OnEquals(a, a));
		
		j2 = Join.factory(
				t1, 
				t2, 
				Lukasiewitz.PRODUCT, 
				Lukasiewitz.INFIMUM, 
				new OnSimilar(a, a, NaiveSimilarity.INTEGER_SIMILARITY));
		
		j3 = Join.factory(
				t1, 
				t2, 
				Lukasiewitz.PRODUCT, 
				Lukasiewitz.INFIMUM, 
				new OnGreaterThanOrEquals(a, a));
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
							Arrays.asList(
									new OnEquals(new Attribute("G", String.class), new Attribute("H", String.class))),
							Lukasiewitz.PRODUCT,
							Lukasiewitz.INFIMUM);
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
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d)));
		
		rslt = j2.eval();
		rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(1, rcrds.size());
		assertTrue(rcrds.contains(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d)));
		
		rslt = j3.eval();
		rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(2, rcrds.size());
		assertTrue(rcrds.contains(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d)));
		assertTrue(rcrds.contains(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 2),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "bar"),
								new Record.AttributeValuePair(c, "baz")), 
						0.7d)));
	}

	/**
	 * Test method for {@link rq.common.operators.Join#schema()}.
	 * @throws DuplicateAttributeNameException 
	 */
	@Test
	void testSchema() throws DuplicateAttributeNameException {
		assertEquals(
				this.expected,
				j1.schema());
	}

}
