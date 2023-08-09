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

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.SchemaNotJoinableException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.latices.Lukasiewitz;
import rq.common.operators.Join;
import rq.common.operators.RecursiveUnrestricted;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.common.operators.Map;
import rq.common.operators.Projection;
import rq.common.operators.Restriction;

class RecursiveUnrestrictedTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	Table t1;
	
	RecursiveUnrestricted ru;
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

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
						new Record.AttributeValuePair(b,"baz")), 
				0.8d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 5), 
						new Record.AttributeValuePair(b,"bah")), 
				0.8d);
		
		t1 = new Table(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		t1.insert(r4);
		
		ru = RecursiveUnrestricted.factory(
				new Restriction(this.t1, r -> r.getNoThrow(a).equals(1)), 
				(Table table) -> {
					try {
						return Projection.factory(
								Join.factory(
										this.t1, 
										new Map(
												table, 
												r -> {
													try {
														return r.set(a, ((int) r.get(a)) + 1);
													} catch (AttributeNotInSchemaException | TypeSchemaMismatchException e) {
														throw new RuntimeException(e);
													}
												}), 
										Arrays.asList(new Join.AttributePair(a, a)), 
										Lukasiewitz.PRODUCT), 
								this.schema).eval();
					} catch (AttributeNotInSchemaException | SchemaNotJoinableException | ComparisonDomainMismatchException
							| NotSubschemaException e) {
						throw new RuntimeException(e);
					}
		});
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testEval() {
		Table rslt = this.ru.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertTrue(rcrds.contains(this.r3));
		assertFalse(rcrds.contains(this.r4));
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.ru.schema());
	}

}
