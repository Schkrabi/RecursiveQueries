package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.latices.Lukasiewitz;
import rq.common.onOperators.OnEquals;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyMapping;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveUnrestricted;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;
import rq.common.interfaces.Table;

class LazyRecursiveUnrestrictedTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 1.0\n"
		+	"3, \"baz\", 0.8\n"
		+	"5, \"bah\", 0.8";
	
	Schema schema;
	Attribute a, b;
	Record r1, r2, r3, r4;
	LazyTable t1;
	
	LazyRecursiveUnrestricted lru;

	@BeforeEach
	void setUp() throws Exception {
		this.t1 = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
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
						new Record.AttributeValuePair(b,"baz")), 
				0.8d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 5), 
						new Record.AttributeValuePair(b,"bah")), 
				0.8d);
		
		Table t = LazyExpression.realizeInMemory(this.t1);
		
		this.lru = LazyRecursiveUnrestricted.factory(
				LazyRestriction.factory(new LazyFacade(t), r -> r.getNoThrow(a).equals(1) ? 1.0d : 0.0d), 
				(Table table) -> {
					try {
						return 
							LazyProjection.factory(
									LazyJoin.factory(
											LazyMapping.factory(
													new LazyFacade(table), 
													r -> {
														try {
															return r.set(a, ((int) r.get(a)) + 1);
														} catch (AttributeNotInSchemaException | TypeSchemaMismatchException e) {
															throw new RuntimeException(e);
														}
													}), 
											new LazyFacade(t), 
											Lukasiewitz.PRODUCT, 
											Lukasiewitz.INFIMUM, 
											new OnEquals(a, a)), 
									new Projection.To(new Attribute("right.A", Integer.class), a),
									new Projection.To(new Attribute("right.B", String.class), b));
					} catch (AttributeNotInSchemaException | DuplicateAttributeNameException e) {
						throw new RuntimeException(e);
					}
				});
	}

	@AfterEach
	void tearDown() throws Exception {
		if(this.t1 != null) {
			this.t1.close();
		}
	}

	@Test
	void testEval() {
		Table rslt = this.lru.eval();
		Set<Record> rcrds = rslt.stream().collect(Collectors.toSet());
		assertEquals(3, rcrds.size());
		assertTrue(rcrds.contains(this.r1));
		assertTrue(rcrds.contains(this.r2));
		assertTrue(rcrds.contains(this.r3));
		assertFalse(rcrds.contains(this.r4));
	}

	@Test
	void testSchema() {
		assertEquals(this.schema, this.lru.schema());
	}

}
