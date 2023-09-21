package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.operators.LazyProjection;
import rq.common.operators.Projection;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;

class LazyProjectionTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.5\n"
		+	"3, \"baz\", 0.7";
	
	Schema schema, subschema, schema2;
	Attribute a, b, c;
	Record r1, r2, r3;
	
	LazyTable table = null, table2 = null;
	LazyProjection p1, p2;

	@BeforeEach
	void setUp() throws Exception {
		table = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		table2 = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.c = new Attribute("C", Integer.class);
		this.schema = Schema.factory(a, b);		
		
		this.subschema = Schema.factory(a);
		this.schema2 = Schema.factory(c, b);
		
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
		
		p1 = LazyProjection.factory(table, subschema);
		p2 = LazyProjection.factory(table2, 
				new Projection.To(a, c), 
				new Projection.To(b, b));
	}

	@AfterEach
	void tearDown() throws Exception {
		if(table != null) {
			table.close();
		}
		if(table2 != null) {
			table2.close();
		}
	}
	
	/**
	 * Test method for {@link rq.common.operators.Projection#factory(rq.common.table.TabularExpression, rq.common.table.Schema)}.
	 */
	@Test
	void testFactory() {
		assertThrows(
				NotSubschemaException.class,
				() -> LazyProjection.factory(
						table, 
						Schema.factory(new Attribute("C", Integer.class)))
				);
	}

	@Test
	void testSchema() {
		assertEquals(this.subschema, this.p1.schema());
		assertEquals(this.schema2, this.p2.schema());
	}

	@Test
	void testNext() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(Record.factory(
				this.subschema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1)), 
				0.8d),
				p1.next());
		assertEquals(Record.factory(
				this.subschema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2)), 
				0.5d),
				p1.next());
		assertEquals(Record.factory(
				this.subschema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3)), 
				0.7d),
				p1.next());
		assertNull(p1.next());
		
		assertEquals(Record.factory(
				this.schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(c, 1), 
						new Record.AttributeValuePair(b, "foo")), 
				0.8d),
				p2.next());
		assertEquals(Record.factory(
				this.schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(c, 2), 
						new Record.AttributeValuePair(b,"bar")),  
				0.5d),
				p2.next());
		assertEquals(Record.factory(
				this.schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(c, 3), 
						new Record.AttributeValuePair(b, "baz")), 
				0.7d),
				p2.next());
		assertNull(p2.next());
	}

}
