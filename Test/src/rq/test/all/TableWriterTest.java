/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.Table;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.io.TableWriter;

/**
 * @author r.skrabal
 *
 */
class TableWriterTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2;
	Table t1;
	
	ByteArrayOutputStream oStream;
	TableWriter writer;

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
		
		t1 = new Table(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		
		oStream = new ByteArrayOutputStream(10000);
		writer = TableWriter.open(oStream);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
		if(writer != null) {
			writer.close();
		}
		if(oStream != null) {
			oStream.close();
		}
	}

	/**
	 * Test method for {@link rq.files.io.TableWriter#write(rq.common.table.Table)}.
	 * @throws ClassNotInContextException 
	 */
	@Test
	void testWrite() throws ClassNotInContextException {
		this.writer.write(t1);
		String rslt = this.oStream.toString();
		assertEquals(
					"A:java.lang.Integer,B:java.lang.String\n"
				+	"1, \"foo\"\n"
				+	"2, \"bar\"",
				rslt);
	}

}
