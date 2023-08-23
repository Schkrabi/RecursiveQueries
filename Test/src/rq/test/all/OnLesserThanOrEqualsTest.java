package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.ComparableDomainMismatchException;
import rq.common.exceptions.NotComparableException;
import rq.common.onOperators.OnLesserThanOrEquals;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;

class OnLesserThanOrEqualsTest {

	Schema schema1, schema2;
	Attribute a, b, c;
	Record r11, r12, r21, r22;
	OnLesserThanOrEquals onLesserThanOrEquals;
	
	@BeforeEach
	void setUp() throws Exception {
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.c = new Attribute("C", String.class);
		schema1 = Schema.factory(a, b);
		schema2 = Schema.factory(a, c);
		r11 = Record.factory(
				schema1, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, "foo")),
				0.8d);
		r12 = Record.factory(
				schema1, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				0.7d);
		r21 = Record.factory(
				schema2, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(c, "baz")),
				1.0d);
		r22 = Record.factory(
				schema2,
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(c, "bah")),
				0.4d);
		
		onLesserThanOrEquals = OnLesserThanOrEquals.factory(a, a);
	}

	@Test
	void testEval() {
		assertEquals(0.0d, onLesserThanOrEquals.eval(r22, r11));
		assertEquals(1.0d, onLesserThanOrEquals.eval(r11, r11));
		assertEquals(1.0d, onLesserThanOrEquals.eval(r11, r22));
	}

	@Test
	void testFactory() {
		assertThrows(
				NotComparableException.class,
				() -> OnLesserThanOrEquals.factory(new Attribute("foo", Object.class), a));
		assertThrows(
				ComparableDomainMismatchException.class,
				() -> OnLesserThanOrEquals.factory(a, b));		
	}

}
