package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.security.spec.ECField;
import java.util.Arrays;
import java.util.function.BiFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.latices.LaticeFactory;
import rq.common.onOperators.Constant;
import rq.common.restrictions.Equals;
import rq.common.restrictions.GreaterThan;
import rq.common.restrictions.GreaterThanOrEquals;
import rq.common.restrictions.InfimumAnd;
import rq.common.restrictions.LesserThan;
import rq.common.restrictions.LesserThanOrEquals;
import rq.common.restrictions.Not;
import rq.common.restrictions.Or;
import rq.common.restrictions.ProductAnd;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.restrictions.SelectionCondition;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;

class SelectionConditionTest {

	Schema schema;
	Attribute a, b;
	Record r1, r2, r3;
	BiFunction<Object, Object, Double> intSimilarity;
	
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
		this.intSimilarity = LinearSimilarity.integerSimilarityUntil(1);
	}

	@Test
	void testIsApplicableToSchmema() throws DuplicateAttributeNameException {
		SelectionCondition c = new LesserThan(a, new Constant<Integer>(1));
		
		assertTrue(c.isApplicableToSchema(this.schema));
		assertFalse(c.isApplicableToSchema(Schema.factory(new Attribute("c", Integer.class))));
	}
	
	@Test
	void testEval() {
		SelectionCondition c = new LesserThan(a, new Constant<Integer>(2));
		assertEquals(1.0d, c.eval(r1));
		assertEquals(0.0d, c.eval(r2));
		
		c = new LesserThanOrEquals(a, new Constant<Integer>(2));
		assertEquals(1.0d, c.eval(r2));
		assertEquals(0.0d, c.eval(r3));
		
		c = new GreaterThan(a, new Constant<Integer>(2));
		assertEquals(1.0d, c.eval(r3));
		assertEquals(0.0d, c.eval(r2));
		
		c = new GreaterThanOrEquals(a, new Constant<Integer>(2));
		assertEquals(1.0d, c.eval(r2));
		assertEquals(0.0d, c.eval(r1));
		
		c = new Equals(a, new Constant<Integer>(2));
		assertEquals(1.0d, c.eval(r2));
		assertEquals(0.0d, c.eval(r3));
		
		c = new Similar(a, new Constant<Integer>(0), this.intSimilarity);
		assertEquals(this.intSimilarity.apply(Integer.valueOf(0), Integer.valueOf(1)), c.eval(r1));
		assertEquals(this.intSimilarity.apply(Integer.valueOf(0), Integer.valueOf(2)), c.eval(r2));
	}
	
	@Test
	void testConnectives() {
		SelectionCondition c = new ProductAnd(
				new LesserThan(a, new Constant<Integer>(2)),
				new LesserThanOrEquals(a, new Constant<Integer>(2)));
		assertEquals(1.0d, c.eval(r1));
		assertEquals(0.0d, c.eval(r2));
		
		Similar similar = new Similar(a, new Constant<Integer>(0), this.intSimilarity);
		c = new ProductAnd(
				similar,
				similar);
		
		double sim = this.intSimilarity.apply(
				Integer.valueOf(0), 
				Integer.valueOf(1));
		assertEquals(LaticeFactory.instance().getProduct().apply(sim, sim), 
				c.eval(r1));
		assertEquals(0.0d, c.eval(r2));
		
		c = new InfimumAnd(
				similar,
				similar);
		
		assertEquals(sim, c.eval(r1));
		assertEquals(0.0d, c.eval(r2));
		
		c = new Or(similar, new LesserThan(a, new Constant<Integer>(2)));
		assertEquals(1.0d, c.eval(r1));
		assertEquals(sim, c.eval(r2));
		
		c = new Not(new LesserThan(a, new Constant<Integer>(2)));
		assertEquals(0.0d, c.eval(r1));
		assertEquals(1.0d, c.eval(r2));
		
		c = new Not(similar);
		assertEquals(1.0d - sim, c.eval(r1));
		
	}

}
