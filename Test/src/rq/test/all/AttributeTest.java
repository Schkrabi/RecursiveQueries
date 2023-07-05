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

import rq.common.table.Attribute;

/**
 * @author r.skrabal
 *
 */
class AttributeTest {
	
	Attribute As = new Attribute("A", String.class);
	Attribute Ai = new Attribute("A", Integer.class);
	Attribute Bs = new Attribute("B", String.class);

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
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.table.Attribute#hashCode()}.
	 */
	@Test
	void testHashCode() {
		assertAll(()-> this.As.hashCode());
		assertEquals(this.As.hashCode(), this.As.hashCode());
		assertNotEquals(this.As.hashCode(), this.Ai.hashCode());
		assertNotEquals(this.As.hashCode(), this.Bs.hashCode());
	}

	/**
	 * Test method for {@link rq.common.table.Attribute#Attribute(java.lang.String, java.lang.Class)}.
	 */
	@Test
	void testAttribute() {
		assertAll(() -> new Attribute("A", Integer.class));
	}

	/**
	 * Test method for {@link rq.common.table.Attribute#toString()}.
	 */
	@Test
	void testToString() {
		String s = this.As.toString();
		assertEquals("A(java.lang.String)", s);
	}

	/**
	 * Test method for {@link rq.common.table.Attribute#equals(java.lang.Object)}.
	 */
	@Test
	void testEqualsObject() {
		assertTrue(this.As.equals(this.As));
		assertFalse(this.As.equals(this.Ai));
		assertFalse(this.As.equals(this.Bs));
		assertTrue(this.As.equalsName(this.Ai));
		assertFalse(this.As.equalsName(this.Bs));
	}

}
