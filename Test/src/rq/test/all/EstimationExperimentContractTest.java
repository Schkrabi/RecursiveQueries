package rq.test.all;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.Test;

import rq.files.contracts.EstimationExperimentContract;

class EstimationExperimentContractTest {

	private EstimationExperimentContract cnt = new EstimationExperimentContract(
			"Top Ranker Real Moves Dataset.csv", "/home/user/test", 3, 50, 1L,
			List.of(new EstimationExperimentContract.AttributeContract("foo", Double.class.getName(), 2.5d, 3, 0.8, 40,
					80.0d, List.of(1.0, 2.0)),
					new EstimationExperimentContract.AttributeContract("bar", Integer.class.getName(), 2.5d, 3, 0.8, 40,
							80.0d, List.of(3.0, 4.0))));
	
	@Test
	void testSerialization() throws IOException {
		var json = cnt.serialize();
		
		var cnt2 = EstimationExperimentContract.deserialize(json);
		assertEquals(cnt, cnt2);
	}

	@Test
	void testGetType() {
		assertEquals(Double.class, cnt.attributes.get(0).getType());
	}
}
