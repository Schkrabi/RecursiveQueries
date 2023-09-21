/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableWriter;

/**
 * @author r.skrabal
 *
 */
class TableWriterTest {
	
	Schema schema;
	Attribute a, b;
	Record r1, r2;
	MemoryTable t1;
	
	ByteArrayOutputStream oStream;
	TableWriter writer;

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
				0.8d);
		
		t1 = new MemoryTable(this.schema);
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
	 * Test method for {@link rq.files.io.TableWriter#write(rq.common.table.MemoryTable)}.
	 * @throws ClassNotInContextException 
	 * @throws IOException 
	 * @throws DuplicateHeaderWriteException 
	 */
	@Test
	void testWrite() throws ClassNotInContextException, IOException, DuplicateHeaderWriteException {
		this.writer.write(t1);		
		String rslt = this.oStream.toString();
		assertEquals(
					"\"A:java.lang.Integer\",\"B:java.lang.String\",\"rank\"\n"
				+	"\"2\",\"bar\",\"0.8\"\n"
				+	"\"1\",\"foo\",\"1.0\"\n",
				rslt);
	}

}
