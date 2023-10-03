package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Iterator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.TopKTable;

class TopKTableTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	List<Record> rcrds;
	TopKTable t;

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
				0.1d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				0.2d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,"baz")), 
				0.3d);
		
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 4),
						new Record.AttributeValuePair(b, "bah")), 
				0.4f);
		
		rcrds = Arrays.asList(r1, r2, r3, r4);
	}

	@Test
	void testFactory() {
		assertAll(() -> TopKTable.factory(schema, 3));
	}

	@Test
	void testMinRank() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		assertEquals(0.0d, this.t.minRank());
		
		t.insert(r1);
		t.insert(r2);
		assertEquals(0.1d, this.t.minRank());
	}

	@Test
	void testIterator() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		Iterator<Record> it = this.t.iterator();
		while(it.hasNext()) {
			assertTrue(this.rcrds.contains(it.next()));
		}
	}

	@Test
	void testInsertRecord() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		assertTrue(t.insert(r1));
		assertTrue(t.insert(r2));
		assertTrue(t.insert(r3));
		
		assertEquals(3, t.size());
		
		assertTrue(t.insert(r4));
		assertEquals(3, t.size());
		assertTrue(t.contains(r2));
		assertTrue(t.contains(r3));
		assertTrue(t.contains(r4));
		assertFalse(t.contains(r1));
	}

	@Test
	void testInsertCollectionOfAttributeValuePairDouble() throws TypeSchemaMismatchException, AttributeNotInSchemaException, TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		assertTrue(t.insert(
					Arrays.asList(
							new Record.AttributeValuePair(a, 2), 
							new Record.AttributeValuePair(b,"bar")),
					0.3d));
	}

	@Test
	void testDelete() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		assertTrue(t.delete(r1));
		assertFalse(t.delete(r3));
		assertTrue(t.contains(r2));
		assertFalse(t.contains(r1));
	}

	@Test
	void testStream() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		Stream<Record> s = t.stream();
		assertNotNull(s);
		assertTrue(s.allMatch(r -> rcrds.contains(r)));
		assertEquals(2, t.stream().collect(Collectors.counting()));
	}

	@Test
	void testContains() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		assertTrue(t.contains(r1));
		assertTrue(t.contains(r2));
		assertFalse(t.contains(r3));
	}

	@Test
	void testContainsNoRank() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		assertTrue(t.containsNoRank(new Record(r1, 1.0d)));
		assertFalse(t.containsNoRank(new Record(r3, 1.0d)));
	}

	@Test
	void testFindNoRank() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		t.insert(r1);
		t.insert(r2);
		
		assertTrue(t.findNoRank(new Record(r1, 1.0d)).isPresent());
		assertFalse(t.findNoRank(new Record(r3, 1.0d)).isPresent());
	}

	@Test
	void testIsEmpty() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		
		assertTrue(t.isEmpty());
		
		t.insert(r1);
		t.insert(r2);
		
		assertFalse(t.isEmpty());
	}

	@Test
	void testSize() throws TableRecordSchemaMismatch {
		this.t = TopKTable.factory(schema, 3);
		
		assertEquals(0, t.size());
		
		t.insert(r1);
		t.insert(r2);
		
		assertEquals(2, t.size());
	}

}
