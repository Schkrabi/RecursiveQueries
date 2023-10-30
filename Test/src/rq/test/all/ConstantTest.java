package rq.test.all;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.onOperators.Constant;
import rq.common.types.Str10;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.table.Attribute;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConstantTest {
	
	Attribute a = new Attribute("a", Integer.class);
	Schema s;
	
	Constant<Integer> ic = new Constant<Integer>(42);
	Constant<Str10> sc = new Constant<Str10>(Str10.factory("foobar"));

	@BeforeEach
	void setUp() throws Exception {
		s = Schema.factory(a);
	}

	@Test
	void testValue() throws TypeSchemaMismatchException, AttributeNotInSchemaException, DuplicateAttributeNameException {
		
		Record record = Record.factory(s, Arrays.asList(new Record.AttributeValuePair(a, 0)), 1.0d);
		assertEquals(42, ic.value(record));
		assertEquals(Str10.factory("foobar"), sc.value(record));
	}

	@Test
	void testIsApplicableToSchema() {
		assertTrue(ic.isApplicableToSchema(s));
		assertTrue(sc.isApplicableToSchema(s));
	}

	@Test
	void testDomain() {
		assertEquals(Integer.class, ic.domain());
		assertEquals(Str10.class, sc.domain());
	}

}
