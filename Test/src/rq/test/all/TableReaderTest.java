/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.opencsv.exceptions.CsvValidationException;

import rq.files.exceptions.ClassNotInContextException;
import rq.files.io.TableReader;
import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.table.Table;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * @author r.skrabal
 *
 */
class TableReaderTest {
	
	String data = 
				"A:java.lang.Integer,B:java.lang.String\n"
			+	"1, \"foo\"\n"
			+	"2, \"bar\"\n"
			+	"3, \"baz\"";
	
	TableReader reader;
	
	Schema schema;
	Attribute a, b;

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
		reader = TableReader.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
		reader.close();
	}

	@Test
	void testRead() throws CsvValidationException, ClassNotFoundException, IOException, TableRecordSchemaMismatch, DuplicateAttributeNameException, ClassNotInContextException, TypeSchemaMismatchException, AttributeNotInSchemaException {
		Table table = reader.read();
		Set<Record> rcrds = table.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(
					Record.factory(
							this.schema,
							Arrays.asList(
									new Record.AttributeValuePair(a, 1), 
									new Record.AttributeValuePair(b, "foo")), 
							1.0d)));
		assertTrue(rcrds.contains(
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 2), 
								new Record.AttributeValuePair(b, "bar")), 
						1.0d)));
		assertTrue(rcrds.contains(
				Record.factory(
						this.schema,
						Arrays.asList(
								new Record.AttributeValuePair(a, 3), 
								new Record.AttributeValuePair(b, "baz")), 
						1.0d)));
	}

}
