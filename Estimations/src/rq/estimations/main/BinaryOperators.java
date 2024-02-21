package rq.estimations.main;

import java.util.Map;
import java.util.function.BinaryOperator;

import rq.common.latices.Godel;
import rq.common.latices.Goguen;
import rq.common.latices.Lukasiewitz;
import java.lang.Math;

public class BinaryOperators {

	private static BinaryOperators singleton = null;
	
	private Map<String, BinaryOperator<Double>> operatorMap = Map.of(
			"max", (x, y) -> (Double)Math.max(x, y),
			"add", (x, y) -> x + y,
			"mult", (x, y) -> x * y,
			"lukasiewitzProduct", Lukasiewitz.PRODUCT,
			"godelProduct", Godel.PRODUCT,
			"goguenProduct", Goguen.PRODUCT
			);
	
	public static BinaryOperators instance() {
		if(singleton == null) {
			singleton = new BinaryOperators();
		}
		return singleton;
	}
	
	private BinaryOperators() {	}

	public static BinaryOperator<Double> get(String name) {
		return instance().operatorMap.get(name);
	}
}
