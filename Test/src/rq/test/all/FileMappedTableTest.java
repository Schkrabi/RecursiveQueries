package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.FileMappedTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.types.Str10;

class FileMappedTableTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	FileMappedTable t1, t2, t3, t4;

	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", Str10.class);
		this.schema = Schema.factory(a, b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, Str10.factory("foo"))),
				0.1d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b, Str10.factory("bar"))), 
				0.2d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, Str10.factory("foo"))), 
				0.3d);
		
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3),
						new Record.AttributeValuePair(b, Str10.factory("baz"))), 
				0.4f);
		
		t1 = FileMappedTable.factory(this.schema);
		t2 = FileMappedTable.factory(this.schema);
		t3 = FileMappedTable.factory(this.schema);
		t4 = FileMappedTable.factory(this.schema);
	}

	@AfterEach
	void tearDown() throws Exception {
		if(this.t1 != null) {
			this.t1.close();
		}
		if(this.t2 != null) {
			this.t2.close();
		}
		if(this.t3 != null) {
			this.t3.close();
		}
		if(this.t4 != null) {
			this.t4.close();
		}
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.t1.schema());
	}

	@Test
	void testInsert() {		
		assertAll(() -> {
			this.t1.insert(this.r1);
			this.t1.insert(
					Arrays.asList(
							new Record.AttributeValuePair(a, 2), 
							new Record.AttributeValuePair(b, Str10.factory("bar"))), 
					1.0d);
			});
	}

	@Test
	void testDelete() {
		this.t3.insert(this.r1);
		this.t3.insert(this.r2);
		this.t3.insert(this.r3);
		
		assertTrue(this.t3.delete(this.r2));
		assertTrue(this.t3.insert(this.r4));
		assertTrue(this.t3.delete(this.r3));
		
		List<Record> recs = Arrays.asList(this.r1, this.r4);
		Iterator<Record> it = this.t3.iterator();
		assertTrue(recs.contains(it.next()));
		assertTrue(recs.contains(it.next()));
		assertThrows(NoSuchElementException.class, () -> it.next());
	}

	@Test
	void testIterator() {
		this.t2.insert(this.r1);
		this.t2.insert(this.r2);
		this.t2.insert(this.r3);
		this.t2.insert(this.r4);
		
		Iterator<Record> it = this.t2.iterator();
		assertNotNull(it);
		
		Record r = it.next();
		assertEquals(this.r1, r);
		assertEquals(this.r2, it.next());
		assertEquals(this.r3, it.next());
		assertEquals(this.r4, it.next());
		assertThrows(NoSuchElementException.class, () -> it.next());
	}

	@Test
	void testStream() {
		this.t2.insert(this.r1);
		this.t2.insert(this.r2);
		this.t2.insert(this.r3);
		
		assertAll(() -> this.t2.stream());
		assertAll(() -> this.t2.stream().forEach(r -> r.toString()));
		assertAll(() -> this.t1.stream().forEach(r -> r.toString()));
	}

	@Test
	void testToString() {
		assertAll(() -> this.t2.toString());
	}

	@Test
	void testContains() {
		this.t2.insert(this.r1);
		this.t2.insert(this.r2);
		this.t2.insert(this.r3);
		
		assertTrue(t2.contains(r1));
		assertFalse(t2.contains(this.r4));
	}

	@Test
	void testContainsNoRank() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		this.t2.insert(this.r1);
		this.t2.insert(this.r2);
		this.t2.insert(this.r3);
		
		assertTrue(t2.contains(r1));
		assertTrue(t2.containsNoRank(new Record(this.r2, 1.0d)));
		assertFalse(t2.contains(this.r4));
	}

	@Test
	void testIsEmpty() {
		this.t2.insert(this.r1);
		this.t2.insert(this.r2);
		this.t2.insert(this.r3);
		
		assertTrue(this.t1.isEmpty());
		assertFalse(this.t2.isEmpty());
	}
}
