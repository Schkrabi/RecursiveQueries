package rq.common.latices;

import java.util.function.BinaryOperator;

public class LaticeFactory {
	
	private BinaryOperator<Double> supremum = Goguen.SUPREMUM;
	private BinaryOperator<Double> infimum = Goguen.INFIMUM;
	private BinaryOperator<Double> product = Goguen.PRODUCT;
	private BinaryOperator<Double> residuum = Goguen.RESIDUUM;
	
	private LaticeFactory() {
		
	}
	
	public BinaryOperator<Double> getSupremum(){
		return this.supremum;
	}
	
	public BinaryOperator<Double> getInfimum(){
		return this.infimum;
	}
	
	public BinaryOperator<Double> getProduct(){
		return this.product;
	}
	
	public BinaryOperator<Double> getResiduu(){
		return this.residuum;
	}
	
	public void setLukasiewitz() {
		this.supremum = Lukasiewitz.SUPREMUM;
		this.infimum = Lukasiewitz.INFIMUM;
		this.product = Lukasiewitz.PRODUCT;
		this.residuum = Lukasiewitz.RESIDUUM;
	}
	
	public void setGodel() {
		this.supremum = Godel.SUPREMUM;
		this.infimum = Godel.INFIMUM;
		this.product = Godel.PRODUCT;
		this.residuum = Godel.RESIDUUM;
	}
	
	public void setGoguen() {
		this.supremum = Goguen.SUPREMUM;
		this.infimum = Goguen.INFIMUM;
		this.product = Goguen.PRODUCT;
		this.residuum = Goguen.RESIDUUM;
	}
	
	private static LaticeFactory singleton = null;

	public static LaticeFactory instance() {
		if(singleton == null) {
			singleton = new LaticeFactory();
		}
		return singleton;
	}
}
