package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.table.Schema;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;

class RecordTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;

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
		Attribute c = new Attribute("C", Integer.class);
		Attribute d = new Attribute("D", String.class);
		r2 = Record.factory(
				Schema.factory(c, d), 
				Arrays.asList(
						new Record.AttributeValuePair(c, 1), 
						new Record.AttributeValuePair(d, "foo")), 
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				1.0d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b,"foo")), 
				0.8d);
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testHashCode() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(this.r1.hashCode(), this.r1.hashCode());
		assertEquals(this.r1.hashCode(),
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 1),
								new Record.AttributeValuePair(b, "foo")),
						1.0d).hashCode());
		
		assertNotEquals(this.r1.hashCode(), this.r2.hashCode());
		assertNotEquals(this.r1.hashCode(), this.r3.hashCode());
		assertNotEquals(this.r1.hashCode(), this.r4.hashCode());
	}

	@Test
	void testRecord() {
		assertThrows(
				TypeSchemaMismatchException.class,
				() -> {
					Record.factory(
							this.schema,
							Arrays.asList(
								new Record.AttributeValuePair(a, "foo"), 
								new Record.AttributeValuePair(b, "bar")),
							1.0d);
				});
	}

	@Test
	void testGet() throws AttributeNotInSchemaException {
		assertEquals("foo", this.r1.get(new Attribute("B", String.class)));
		assertThrows(
				AttributeNotInSchemaException.class,
				() -> {
					this.r1.get(new Attribute("C", Integer.class));
				});
		assertEquals("foo", this.r1.get("B"));
		assertThrows(
				AttributeNotInSchemaException.class,
				() -> {
					this.r1.get("C");
				});
	}

	@Test
	void testToString() {
		assertEquals("[A(java.lang.Integer): 1, B(java.lang.String): foo; 1.0]", this.r1.toString());
	}

	@Test
	void testEqualsObject() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(this.r1, this.r1);
		assertEquals(this.r1,
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 1),
								new Record.AttributeValuePair(b, "foo")),
						1.0d));
		
		assertNotEquals(this.r1, this.r2);
		assertNotEquals(this.r1, this.r3);
		assertNotEquals(this.r1, this.r4);
	}
	
	@Test
	void testEqualsNoRank() {
		assertTrue(this.r1.equalsNoRank(this.r1));
		assertFalse(this.r1.equalsNoRank(this.r2));
		assertFalse(this.r1.equalsNoRank(this.r3));
		assertTrue(this.r1.equalsNoRank(this.r4));
	}

	@Test
	void testSet() throws AttributeNotInSchemaException, TypeSchemaMismatchException {
		assertThrows(AttributeNotInSchemaException.class, 
				() -> r1.set(new Attribute("C", Integer.class), "foo"));
		assertThrows(TypeSchemaMismatchException.class,
				() -> r1.set(this.a, "foo"));
		Record s = r1.set(this.b, "bar");
		assertNotNull(s);
		assertEquals(s.get(a), 1);
		assertEquals(s.get(b), "bar");
	}
}
