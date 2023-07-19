/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.Table;

/**
 * @author r.skrabal
 *
 */
class TableTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	Table t1, t2, t3, t4;

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
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b,"foo")), 
				0.8d);
		
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3),
						new Record.AttributeValuePair(b, "baz")), 
				1.0f);
		
		
		t1 = new Table(this.schema);
		t2 = new Table(this.schema);
		t2.insert(r1);
		t2.insert(r2);
		t2.insert(r3);
		
		t3 = new Table(this.schema);
		t3.insert(r1);
		
		t4 = new Table(this.schema);
		t4.insert(r1);
		t4.insert(r2);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.table.Table#insert(rq.common.table.Record)}.
	 * @throws TableRecordSchemaMismatch 
	 * @throws TypeSchemaMismatchException 
	 */
	@Test
	void testInsert() throws TableRecordSchemaMismatch, TypeSchemaMismatchException {
		assertAll(() -> {
			this.t1.insert(this.r1);
			this.t1.insert(
					Arrays.asList(
							new Record.AttributeValuePair(a, 2), 
							new Record.AttributeValuePair(b, "bar")), 
					1.0d);
			});
		
		assertThrows(
				TableRecordSchemaMismatch.class,
				() -> {
					Attribute d = new Attribute("D", Integer.class);
					Attribute e = new Attribute("E", String.class);
					this.t1.insert(
							Record.factory(
									Schema.factory(d, e), 
									Arrays.asList(
											new Record.AttributeValuePair(d, 1), 
											new Record.AttributeValuePair(e, "foo")), 
									1.0d));
				});
		assertThrows(
				TypeSchemaMismatchException.class,
				() -> {
					this.t1.insert(
							Arrays.asList(
									new Record.AttributeValuePair(a, "baz"), 
									new Record.AttributeValuePair(b, 3.0f)), 
							1.0);
				});
	}

	/**
	 * Test method for {@link rq.common.table.Table#delete(rq.common.table.Record)}.
	 * @throws TableRecordSchemaMismatch 
	 */
	@Test
	void testDelete() throws TableRecordSchemaMismatch {
		assertTrue(this.t2.delete(this.r1));
		assertFalse(this.t2.delete(this.r1));
		assertThrows(TableRecordSchemaMismatch.class,
				() -> {
					Attribute d = new Attribute("D", Integer.class);
					Attribute e = new Attribute("E", String.class);
					this.t2.delete(Record.factory(
							Schema.factory(d, e), 
							Arrays.asList(
									new Record.AttributeValuePair(d, 1), 
									new Record.AttributeValuePair(e, "foo")), 
							1.0d));
				});
	}

	/**
	 * Test method for {@link rq.common.table.Table#update(rq.common.table.Record, rq.common.table.Record)}.
	 * @throws TableRecordSchemaMismatch 
	 */
	@Test
	void testUpdate() throws TableRecordSchemaMismatch {
		assertTrue(this.t3.update(this.r1, this.r2));
		assertFalse(this.t3.update(this.r1, this.r3));
		this.t3.insert(this.r1);		
		assertFalse(this.t3.update(this.r2, this.r1));
		
		Attribute d = new Attribute("D", Integer.class);
		Attribute e = new Attribute("E", String.class);
		
		assertThrows(TableRecordSchemaMismatch.class,
				() -> {
					this.t3.update(Record.factory(
							Schema.factory(d, e), 
							Arrays.asList(
									new Record.AttributeValuePair(d, 1), 
									new Record.AttributeValuePair(e, "foo")), 
							1.0d), 
						this.r1);
				});
		assertThrows(TableRecordSchemaMismatch.class,
				() -> {
					this.t3.update(this.r1,
							Record.factory(
								Schema.factory(
										new Attribute("D", Integer.class),
										new Attribute("E", String.class)), 
								Arrays.asList(
										new Record.AttributeValuePair(d, 1), 
										new Record.AttributeValuePair(e, "foo")), 
								1.0d));
				});
	}
	
	@Test
	void testContainsNoRank() {
		assertTrue(this.t4.containsNoRank(this.r1));;
		assertFalse(this.t4.containsNoRank(this.r4));
		assertTrue(this.t4.containsNoRank(this.r3));
	}

	@Test
	void testFindNoRank() {
		assertEquals(Optional.of(this.r1), this.t4.findNoRank(r1));
		assertTrue(this.t4.findNoRank(this.r4).isEmpty());
		assertEquals(Optional.of(this.r1), this.t4.findNoRank(this.r3));
	}
}
