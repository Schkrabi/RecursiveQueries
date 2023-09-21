/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.table.Attribute;
import rq.common.table.CachedExpression;
import rq.common.table.Schema;
import rq.files.io.LazyTable;
import rq.common.interfaces.LazyIterator;
import rq.common.table.Record;

/**
 * @author r.skrabal
 *
 */
class CachedExpressionTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.5\n"
		+	"3, \"baz\", 0.7";
	
	Schema schema;
	Attribute a, b;
	
	LazyTable table = null;
	CachedExpression c;
	
	Set<Record> records;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		table = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
		c = CachedExpression.factory(table);
		
		records = new HashSet<Record>();
		records.add(
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 1), 
								new Record.AttributeValuePair(b, "foo")), 
						0.8d));
		records.add(
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 2), 
								new Record.AttributeValuePair(b, "bar")), 
						0.5d));
		records.add(
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 3), 
								new Record.AttributeValuePair(b, "baz")), 
						0.7d));
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

	/**
	 * Test method for {@link rq.common.table.CachedExpression#lazyIterator()}.
	 */
	@Test
	void testLazyIterator() {
		LazyIterator it = c.lazyIterator();
		Record r = it.next();
		while(r != null) {
			this.records.contains(r);
			r = it.next();
		}
		
		it = c.lazyIterator();
		r = it.next();
		while(r != null) {
			this.records.contains(r);
			r = it.next();
		}
	}

	/**
	 * Test method for {@link rq.common.table.CachedExpression#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.schema, c.schema());
	}

}
