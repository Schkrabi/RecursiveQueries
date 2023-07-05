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
	Record r1, r2, r3, r4;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
		this.schema = Schema.factory(
				new Attribute("A", Integer.class),
				new Attribute("B", String.class));
		
		r1 = Record.factory(
				this.schema,
				Arrays.asList(1, "foo"),
				1.0d);
		r2 = Record.factory(
				Schema.factory(
						new Attribute("C", Integer.class),
						new Attribute("D", String.class)), 
				Arrays.asList(1, "foo"), 
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(2, "bar"), 
				1.0d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(1, "foo"), 
				0.8d);
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testHashCode() {
		assertEquals(this.r1.hashCode(), this.r1.hashCode());
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
							Arrays.asList("foo", "bar"),
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
	}

	@Test
	void testToString() {
		assertEquals("A(java.lang.Integer): 1, B(java.lang.String): foo", this.r1.toString());
	}

	@Test
	void testEqualsObject() {
		assertEquals(this.r1, this.r1);
		assertNotEquals(this.r1, this.r2);
		assertNotEquals(this.r1, this.r3);
		assertNotEquals(this.r1, this.r4);
	}

}
