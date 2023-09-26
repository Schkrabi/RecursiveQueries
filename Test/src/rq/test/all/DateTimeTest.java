/**
 * 
 */
package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.types.DateTime;

/**
 * @author r.skrabal
 *
 */
class DateTimeTest {
	
	Random random = new Random();
	LocalDateTime localDateTime;

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
		localDateTime = LocalDateTime.of(
				random.nextInt(2000) + 1,
				random.nextInt(12) + 1,
				random.nextInt(30) + 1,
				random.nextInt(24),
				random.nextInt(60),
				random.nextInt(60),
				random.nextInt(1000));
	}

	/**
	 * Test method for {@link rq.common.types.DateTime#DateTime(java.time.LocalDateTime)}.
	 */
	@Test
	void testDateTime() {
		assertAll(() -> new DateTime(this.localDateTime));
	}

	/**
	 * Test method for {@link rq.common.types.DateTime#toBytes()}.
	 */
	@Test
	void testToBytes() {
		DateTime dt = new DateTime(this.localDateTime);
		byte[] btes = null;
		
		btes = dt.toBytes();
		assertNotNull(btes);
		
		DateTime dt2 = new DateTime();
		dt2.fromBytes(btes);
		assertEquals(dt, dt2);
	}

	/**
	 * Test method for {@link rq.common.types.DateTime#byteArraySize()}.
	 */
	@Test
	void testByteArraySize() {
		DateTime dt = new DateTime(this.localDateTime);
		assertTrue(dt.byteArraySize() > 0);
	}

}
