/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.operators.Restriction;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.MemoryTable;
import rq.common.interfaces.Table;

/**
 * @author Mgr. R.Skrabal
 *
 */
class RestrictionTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	MemoryTable t1;
	Restriction s1;
	
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
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,"foo")), 
				0.8d);
		
		t1 = new MemoryTable(this.schema);
		t1.insert(r1);
		t1.insert(r2);
		t1.insert(r3);
		
		s1 = new Restriction(this.t1, r -> (Integer)r.getNoThrow(a) < 3 ? r.rank : 0.0d);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.operators.Restriction#eval()}.
	 * @throws TableRecordSchemaMismatch 
	 */
	@Test
	void testEval() throws TableRecordSchemaMismatch {
		Table rslt = this.s1.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(2, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertFalse(rcrds.contains(this.r3));
	}

	/**
	 * Test method for {@link rq.common.operators.Restriction#schema()}.
	 */
	@Test
	void testSchema() {
		assertEquals(this.t1.schema, s1.schema());
	}

}
