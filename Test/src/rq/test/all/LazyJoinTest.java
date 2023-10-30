package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.TypeSchemaMismatchException;
import rq.common.latices.Lukasiewitz;
import rq.common.onOperators.OnEquals;
import rq.common.onOperators.OnGreaterThanOrEquals;
import rq.common.onOperators.OnSimilar;
import rq.common.operators.LazyJoin;
import rq.common.similarities.NaiveSimilarity;
import rq.common.table.Attribute;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.files.io.LazyTable;

class LazyJoinTest {
	
	String data1 = 
			"A:java.lang.Integer,B:java.lang.String,rank\n"
		+	"1, \"foo\", 0.8\n"
		+	"2, \"bar\", 0.7";
	
	String data2 =
			"A:java.lang.Integer,C:java.lang.String,rank\n"
		+	"1, \"baz\", 1.0\n"
		+	"3, \"bah\", 0.4";
	
	Schema schema1, schema2, expected;
	Attribute a, b, c, la, ra;
	Record r11, r12, r21, r22;
	LazyTable t1, t2, u1, u2, v1, v2;
	LazyJoin j1, j2, j3;

	@BeforeEach
	void setUp() throws Exception {
		t1 = LazyTable.open(new ByteArrayInputStream(this.data1.getBytes()));
		u1 = LazyTable.open(new ByteArrayInputStream(this.data1.getBytes()));
		v1 = LazyTable.open(new ByteArrayInputStream(this.data1.getBytes()));
		t2 = LazyTable.open(new ByteArrayInputStream(this.data2.getBytes()));
		u2 = LazyTable.open(new ByteArrayInputStream(this.data2.getBytes()));
		v2 = LazyTable.open(new ByteArrayInputStream(this.data2.getBytes()));
		
		this.a = new Attribute("A", Integer.class);
		this.b = new Attribute("B", String.class);
		this.c = new Attribute("C", String.class);
		this.la = new Attribute("left." + a.name, a.domain);
		this.ra = new Attribute("right." + a.name, a.domain);
		schema1 = Schema.factory(a, b);
		schema2 = Schema.factory(a, c);
		expected = Schema.factory(
					la,
					ra,
					b, 
					c);
		
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
		
		j1 = LazyJoin.factory(
				t1, 
				t2,
				Lukasiewitz.PRODUCT,
				Lukasiewitz.INFIMUM,
				new OnEquals(a, a));
		j2 = LazyJoin.factory(
				u1, 
				u2, 
				Lukasiewitz.PRODUCT, 
				Lukasiewitz.INFIMUM, 
				new OnSimilar(a, a, NaiveSimilarity.INTEGER_SIMILARITY));
		j3 = LazyJoin.factory(
				v1, 
				v2, 
				Lukasiewitz.PRODUCT, 
				Lukasiewitz.INFIMUM, 
				new OnGreaterThanOrEquals(a, a));
	}

	@AfterEach
	void tearDown() throws Exception {
		if(t1 != null) {
			t1.close();
		}
		if(t2 != null) {
			t2.close();
		}
		if(u1 != null) {
			u1.close();
		}
		if(u2 != null) {
			u2.close();
		}
		if(v1 != null) {
			v1.close();
		}
		if(v2 != null) {
			v2.close();
		}
	}

	@Test
	void testFactory() {
		assertThrows(
				OnOperatornNotApplicableToSchemaException.class,
				() -> {
					LazyJoin.factory(
							t1, 
							t2,
							Arrays.asList(
									new OnEquals(new Attribute("G", String.class), new Attribute("H", String.class))),
							Lukasiewitz.PRODUCT,
							Lukasiewitz.INFIMUM);
				});
	}

	@Test
	void testSchema() {
		assertEquals(
				this.expected,
				j1.schema());
		
	}

	@Test
	void testNext() throws TypeSchemaMismatchException, AttributeNotInSchemaException {
		assertEquals(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d),
				this.j1.next());
		assertNull(this.j1.next());
		
		assertEquals(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d),
				this.j2.next());
		assertNull(this.j2.next());
		
		assertEquals(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 1),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "foo"),
								new Record.AttributeValuePair(c, "baz")), 
						0.8d),
				this.j3.next());
		assertEquals(
				Record.factory(this.expected, 
						Arrays.asList(
								new Record.AttributeValuePair(la, 2),
								new Record.AttributeValuePair(ra, 1),
								new Record.AttributeValuePair(b, "bar"),
								new Record.AttributeValuePair(c, "baz")), 
						0.7d),
				this.j3.next());
		assertNull(this.j3.next());
	}

}
