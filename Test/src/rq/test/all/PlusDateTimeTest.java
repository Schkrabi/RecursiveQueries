package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.onOperators.PlusDateTime;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.types.DateTime;

class PlusDateTimeTest {
	
	Attribute a = new Attribute("a", DateTime.class);
	Schema s;
	
	PlusDateTime pd = new PlusDateTime(a, Duration.ofDays(1));

	@BeforeEach
	void setUp() throws Exception {
		s = Schema.factory(a);
	}

	@Test
	void testValue() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		Record record = Record.factory(s, 
				Arrays.asList(new Record.AttributeValuePair(a, new DateTime(LocalDateTime.of(2023, 1, 1, 0, 0)))), 
				1.0d);
		
		assertEquals(new DateTime(LocalDateTime.of(2023, 1, 2, 0, 0)), pd.value(record));
	}

	@Test
	void testIsApplicableToSchema() {
		assertTrue(pd.isApplicableToSchema(s));
	}

	@Test
	void testDomain() {
		assertEquals(DateTime.class, pd.domain());
	}

}
