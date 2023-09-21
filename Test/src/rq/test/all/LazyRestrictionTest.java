package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.operators.LazyRestriction;
import rq.common.table.Attribute;
import rq.common.table.Schema;
import rq.files.io.LazyTable;
import rq.common.table.Record;

class LazyRestrictionTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.5\n"
		+	"3, \"baz\", 0.7";
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	
	LazyTable table = null;
	LazyRestriction r;

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
		
		this.r = LazyRestriction.factory(
				table, 
				r -> r.rank > 0.5d ? r.rank : 0.0d);
	}

	@AfterEach
	void tearDown() throws Exception {
		if(table != null) {
			table.close();
		}
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.r.schema());
	}

	@Test
	void testNext() {
		Record record = this.r.next();
		assertEquals(r1, record);
		record = this.r.next();
		assertEquals(r3, record);
		assertNull(this.r.next());
	}

}
