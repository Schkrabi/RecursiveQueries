package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.RecordValueNotApplicableOnSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.interfaces.LazyExpression;
import rq.common.interfaces.Table;
import rq.common.latices.Lukasiewitz;
import rq.common.onOperators.Constant;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.PlusInteger;
import rq.common.operators.Join;
import rq.common.operators.LazyJoin;
import rq.common.operators.LazyMapping;
import rq.common.operators.LazyProjection;
import rq.common.operators.LazyRecursiveTransformed;
import rq.common.operators.LazyRestriction;
import rq.common.operators.Projection;
import rq.common.table.Attribute;
import rq.common.table.LazyFacade;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;

class LazyRecursiveTransformedTest {
	
	String data = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.7\n"
		+	"2, \"bar\", 0.8\n"
		+	"3, \"baz\", 0.9\n"
		+	"5, \"bah\", 1.0";
	
	Schema schema, schema2;
	Attribute a, b;
	Record r1, r2, r3, r4, rp1, rp2, rp3, rp4;
	LazyTable t1;
	
	LazyRecursiveTransformed lrt;

	@BeforeEach
	void setUp() throws Exception {
		this.t1 = LazyTable.open(new ByteArrayInputStream(this.data.getBytes()));
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.schema = Schema.factory(a, b);
		this.schema2 = Schema.factory(b);
		r1 = Record.factory(
				this.schema,
				Arrays.asList(
						new Record.AttributeValuePair(a, 1), 
						new Record.AttributeValuePair(b, "foo")),
				0.7d);
		rp1 = Record.factory(
				schema2, 
				Arrays.asList(new Record.AttributeValuePair(b, "foo")),
				1.0d);
		r2 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 2), 
						new Record.AttributeValuePair(b,"bar")), 
				0.8d);
		rp2 = Record.factory(
				schema2, 
				Arrays.asList(new Record.AttributeValuePair(b, "bar")),
				1.0d);
		r3 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 3), 
						new Record.AttributeValuePair(b,"baz")), 
				0.9d);
		rp3 = Record.factory(
				schema2, 
				Arrays.asList(new Record.AttributeValuePair(b, "baz")),
				1.0d);
		r4 = Record.factory(
				schema, 
				Arrays.asList(
						new Record.AttributeValuePair(a, 5), 
						new Record.AttributeValuePair(b,"bah")), 
				1.0d);
		rp4 = Record.factory(
				schema2, 
				Arrays.asList(new Record.AttributeValuePair(b, "bah")),
				1.0d);
		
		Table t = LazyExpression.realizeInMemory(this.t1);
		
		this.lrt = LazyRecursiveTransformed.factory(
				LazyRestriction.factory(new LazyFacade(t), r -> r.getNoThrow(a).equals(1) ? r.rank : 0.0d), 
				(Table table) -> {
					try {
						return 
							LazyProjection.factory(
									LazyJoin.factory(
											new LazyFacade(table), 
											new LazyFacade(t),  
											new OnEquals(new PlusInteger(a, new Constant<Integer>(1)), a)), 
									new Projection.To(Join.right(a), a),
									new Projection.To(Join.right(b), b));
					} catch (DuplicateAttributeNameException | OnOperatornNotApplicableToSchemaException | RecordValueNotApplicableOnSchemaException e) {
						throw new RuntimeException(e);
					}
				}, 
				2, 
				(Record r) -> {
					try {
						return LazyProjection.factory(new LazyFacade(MemoryTable.of(r)), new Projection.To(b, b));
					} catch (DuplicateAttributeNameException | RecordValueNotApplicableOnSchemaException e) {
						// Unlikely
						throw new RuntimeException(e);
					}
				});
	}

	@Test
	void testEval() {
		Table rslt = this.lrt.eval();
		assertEquals(2, rslt.size());
		assertTrue(rslt.containsNoRank(this.rp1));
		assertTrue(rslt.containsNoRank(this.rp2));
		assertFalse(rslt.contains(this.rp3));
		assertFalse(rslt.contains(this.rp4));
	}

	@Test
	void testSchema() {
		assertEquals(this.schema2, this.lrt.schema());
	}

}
