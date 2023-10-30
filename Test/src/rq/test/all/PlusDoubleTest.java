package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.onOperators.Constant;
import rq.common.onOperators.PlusDouble;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;

class PlusDoubleTest {
	
	Attribute a = new Attribute("a", Double.class);
	Schema s;
	
	PlusDouble pd = new PlusDouble(a, new Constant<Double>(3.0d));

	@BeforeEach
	void setUp() throws Exception {
		s = Schema.factory(a);
	}

	@Test
	void testValue() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		Record record = Record.factory(s, Arrays.asList(new Record.AttributeValuePair(a, 2.0d)), 1.0d);
		assertEquals(5.0d, pd.value(record));
	}

	@Test
	void testIsApplicableToSchema() {
		assertTrue(pd.isApplicableToSchema(s));
	}

	@Test
	void testDomain() {
		assertEquals(Double.class, pd.domain());
	}

}
