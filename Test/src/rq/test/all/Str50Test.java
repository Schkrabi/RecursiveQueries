package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Random;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.types.Str10;
import rq.common.types.Str50;

class Str50Test {
	Random random = new Random();
	byte[] btes = new byte[160];
	String str;
	
	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		random.nextBytes(btes);
		str = new String("blah");
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	/**
	 * Test method for {@link rq.common.types.Str10#factory(java.lang.String)}.
	 */
	@Test
	void testFactory() {
		assertAll(() -> Str10.factory(str));
	}

	/**
	 * Test method for {@link rq.common.types.Str10#compareTo(rq.common.types.Str10)}.
	 */
	@Test
	void testCompareTo() {
		Str50 s1 = Str50.factory("aaa");
		Str50 s2 = Str50.factory("bbb");
		assertEquals(s1, s1);
		assertEquals(s1.hashCode(), s1.hashCode());
		assertNotEquals(s1, s2);
		assertNotEquals(s1.hashCode(), s2.hashCode());
		
		assertEquals(0, s1.compareTo(s1));
		assertEquals("aaa".compareTo("bbb"), s1.compareTo(s2));
		assertEquals("bbb".compareTo("aaa"), s2.compareTo(s1));
	}

	/**
	 * Test method for {@link rq.common.types.Str10#toBytes()}.
	 */
	@Test
	void testToBytes() {
		Str50 s = Str50.factory(str);
		byte[] buff = s.toBytes();
		assertNotNull(buff);
		
		Str50 r = new Str50();
		r.fromBytes(buff);
		assertEquals(s, r);
	}

	/**
	 * Test method for {@link rq.common.types.Str10#byteArraySize()}.
	 */
	@Test
	void testByteArraySize() {
		assertEquals(160, Str50.factory(str).byteArraySize());
	}

}
