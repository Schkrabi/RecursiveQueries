package rq.test.all;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import rq.common.similarities.NaiveSimilarity;

class NaiveSimilarityTest {
	
	Random rand = new Random();

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	@BeforeEach
	void setUp() throws Exception {
	}

	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	void testIntegerSimilarity() {
		int a = rand.nextInt();
		int b = rand.nextInt();
		int c = rand.nextInt();
		
		assertEquals(1.0d, NaiveSimilarity.INTEGER_SIMILARITY.apply(a, a));
		assertEquals(NaiveSimilarity.INTEGER_SIMILARITY.apply(b, c), NaiveSimilarity.INTEGER_SIMILARITY.apply(c, b));
	}

	@Test
	void testDoubleSimilarity() {
		double a = rand.nextDouble();
		double b = rand.nextDouble();
		double c = rand.nextDouble();
		
		assertEquals(1.0d, NaiveSimilarity.DOUBLE_SIMILARITY.apply(a, a));
		assertEquals(NaiveSimilarity.DOUBLE_SIMILARITY.apply(b, c), NaiveSimilarity.DOUBLE_SIMILARITY.apply(c, b));
	}
}
