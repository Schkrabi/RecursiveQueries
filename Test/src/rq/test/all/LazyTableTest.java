package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;

class LazyTableTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.5\n"
		+	"3, \"baz\", 0.7";
	
	Schema schema;
	Attribute a, b;
	
	LazyTable table = null;

	@BeforeEach
	void setUp() throws Exception {
		table = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
	}
	
	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
		if(table != null) {
			table.close();
		}
	}

	@Test
	void testNext() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(table.next(),
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 1), 
								new Record.AttributeValuePair(b, "foo")), 
						0.8d));
		assertEquals(table.next(),
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 2), 
								new Record.AttributeValuePair(b, "bar")), 
						0.5d));
		assertEquals(table.next(),
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 3), 
								new Record.AttributeValuePair(b, "baz")), 
						0.7d));
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, table.schema());
	}

}
