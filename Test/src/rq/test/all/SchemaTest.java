package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.table.Attribute;
import rq.common.table.Schema;

class SchemaTest {

	private Schema s1;
	private Schema s2;
	private Schema s3;
	
	
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
		this.s1 = Schema.factory(
				new Attribute("A", Integer.class), 
				new Attribute("B", String.class));
		this.s2 = Schema.factory(
				new Attribute("A", String.class),
				new Attribute("B", String.class));
		this.s3 = Schema.factory(
				new Attribute("C", Integer.class),
				new Attribute("B", String.class));
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testHashCode() {
		assertEquals(this.s1.hashCode(), this.s1.hashCode());
		assertNotEquals(this.s1.hashCode(), this.s2.hashCode());
		assertNotEquals(this.s1.hashCode(), this.s3.hashCode());
	}

	@Test
	void testSchema() {
		assertThrows(DuplicateAttributeNameException.class,() -> {
			Schema.factory(new Attribute("A", Integer.class), new Attribute("A", String.class));
		});
	}

	@Test
	void testAttributeIndex() {
		assertEquals(Optional.of(0), this.s1.attributeIndex(new Attribute("A", Integer.class)));
	}

	@Test
	void testToString() {
		assertEquals("A(java.lang.Integer)B(java.lang.String)", this.s1.toString());
	}

	@Test
	void testEqualsObject() {
		assertEquals(this.s1, this.s1);
		assertNotEquals(this.s1, this.s2);
		assertNotEquals(this.s1, this.s3);
	}

}
