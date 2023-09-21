package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.operators.LazyMapping;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;

class LazyMappingTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.5\n"
		+	"3, \"baz\", 0.7";
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	
	LazyTable table = null;
	
	LazyMapping m;

	@BeforeEach
	void setUp() throws Exception {
		table = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
		
		this.r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, "foo")), 
				0.8d);
		this.r2 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b, "bar")), 
				0.5d);
		this.r3 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b, "baz")), 
				0.7d);
		m = LazyMapping.factory(table,
				r -> {
					try {
						return r.set(a, ((int)r.get(a)) + 1);
					} catch (AttributeNotInSchemaException | TypeSchemaMismatchException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@AfterEach
	void tearDown() throws Exception {
		if(table != null) {
			table.close();
		}
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.m.schema());
	}

	@Test
	void testNext() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 2),
								new Record.AttributeValuePair(b, "foo")), 
						0.8d),
				this.m.next());
		assertEquals(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 3),
								new Record.AttributeValuePair(b, "bar")), 
						0.5d),
				this.m.next());
		assertEquals(
				Record.factory(
						this.schema, 
						Arrays.asList(
								new Record.AttributeValuePair(a, 4),
								new Record.AttributeValuePair(b, "baz")), 
						0.7d),
				this.m.next());
		assertNull(this.m.next());
	}

}
