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
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.operators.Map;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.interfaces.Table;

class MapTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	Map m1;
	
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
						new Record.AttributeValuePair(b,"foo")), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		m1 = new Map(t1, r -> {
			try {
				return r.set(a, ((int)r.get(a)) + 1);
			} catch (AttributeNotInSchemaException | TypeSchemaMismatchException e) {
				throw new RuntimeException(e);
			}
		});
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testEval() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		Table rslt = this.m1.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 2),
								new Record.AttributeValuePair(b, "foo")), 
						1.0d)));
		assertTrue(rcrds.contains(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 3),
								new Record.AttributeValuePair(b, "bar")), 
						1.0d)));
		assertTrue(rcrds.contains(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 4),
								new Record.AttributeValuePair(b, "foo")), 
						0.8d)));
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.m1.schema());
	}

}
