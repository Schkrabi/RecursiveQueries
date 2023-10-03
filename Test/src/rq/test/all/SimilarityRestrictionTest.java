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

import rq.common.latices.Lukasiewitz;
import rq.common.operators.SimilarityRestriction;
import rq.common.similarities.NaiveSimilarity;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.Table;

/**
 * @author Mgr. R.Skrabal
 *
 */
class SimilarityRestrictionTest {

	Schema schema;
	Attribute a1, a2, b;
	Record r1, r2, r3;
	MemoryTable t1;
	SimilarityRestriction s1;
	
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
		this.a1 = new Attribute("A1", Integer.class);
		this.a2 = new Attribute("A2", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a1, a2, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a1, 1),
						new Record.AttributeValuePair(a2, 2),
						new Record.AttributeValuePair(b, "foo")),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a1, 2),
						new Record.AttributeValuePair(a2, 4),
						new Record.AttributeValuePair(b, "bar")),
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a1, 3),
						new Record.AttributeValuePair(a2, 3),
						new Record.AttributeValuePair(b, "baz")),
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		s1 = SimilarityRestriction.factory(
				t1, 
				new Attribute("A1", Integer.class),
				new Attribute("A2", Integer.class), 
				Lukasiewitz.PRODUCT, 
				NaiveSimilarity.INTEGER_SIMILARITY);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.SimilarityRestriction#factory(rq.common.table.TabularExpression, rq.common.table.Attribute, rq.common.table.Attribute, java.util.function.BiFunction, java.util.function.BiFunction)}.
	 */
	@Test
	void testFactory() {
		assertThrows(
				AttributeNotInSchemaException.class,
				() -> {
					SimilarityRestriction.factory(
							t1, 
							new Attribute("Q", Integer.class), 
							new Attribute("A2", Integer.class), 
							Lukasiewitz.PRODUCT, 
							NaiveSimilarity.INTEGER_SIMILARITY);
				});
		assertThrows(
				AttributeNotInSchemaException.class,
				() -> {
					SimilarityRestriction.factory(
							t1, 
							new Attribute("A1", Integer.class), 
							new Attribute("Q", Integer.class), 
							Lukasiewitz.PRODUCT, 
							NaiveSimilarity.INTEGER_SIMILARITY);
				});
		assertThrows(
				ComparisonDomainMismatchException.class,
				() -> {
					SimilarityRestriction.factory(
							t1, 
							new Attribute("A1", Integer.class), 
							new Attribute("B", String.class), 
							Lukasiewitz.PRODUCT, 
							NaiveSimilarity.INTEGER_SIMILARITY);
				});
	}

	/**
	 * Test method for {@link rq.common.operators.SimilarityRestriction#eval()}.
	 * @throws TypeSchemaMismatchException 
	 * @throws AttributeNotInSchemaException 
	 */
	@Test
	void testEval() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		Table table = this.s1.eval();
		Set<Record> rcrds = table.stream().collect(Collectors.toSet());
		assertTrue(rcrds.size() == 3);
		assertTrue(table.containsNoRank(r1));
//		.contains(Record.factory(
//						this.schema, 
//						Arrays.asList(
//								new Record.AttributeValuePair(a1, 1),
//								new Record.AttributeValuePair(a2, 2),
//								new Record.AttributeValuePair(b, "foo")),
//						0.25d)));
		assertTrue(rcrds.contains(Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a1, 2),
								new Record.AttributeValuePair(a2, 4),
								new Record.AttributeValuePair(b, "bar")),
						0d)));
		assertTrue(rcrds.contains(Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a1, 3),
								new Record.AttributeValuePair(a2, 3),
								new Record.AttributeValuePair(b, "baz")),
						0.8d)));
	}

	/**
	 * Test method for {@link rq.common.operators.SimilarityRestriction#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.t1.schema, this.s1.schema());
	}

}
