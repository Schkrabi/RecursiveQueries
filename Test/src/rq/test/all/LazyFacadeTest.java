package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;

class LazyFacadeTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t;
	
	LazyFacade facade;

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
						new Record.AttributeValuePair(b, "baz")), 
				1.0f);
		
		t = new MemoryTable(this.schema);
		t.insert(r1);
		t.insert(r2);
		t.insert(r3);
	}

	@Test
	void testLazyFacade() {
		assertAll(() -> new LazyFacade(this.t));
	}

	@Test
	void testSchema() {
		this.facade = new LazyFacade(this.t);
		assertEquals(this.t.schema, this.facade.schema());
	}

	@Test
	void testNext() {
		this.facade = new LazyFacade(this.t);
		List<Record> l = Arrays.asList(this.r1, this.r2, this.r3);
		
		assertTrue(l.contains(this.facade.next()));
		assertTrue(l.contains(this.facade.next()));
		assertTrue(l.contains(this.facade.next()));
		assertNull(this.facade.next());
		assertNull(this.facade.next());
	}

}
