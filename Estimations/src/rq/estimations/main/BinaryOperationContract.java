package rq.estimations.main;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

import rq.common.interfaces.Table;
import rq.common.latices.Goguen;
import rq.files.io.TableReader;

public class BinaryOperationContract extends EstimationSetupContract {

	private Table left;
	private Table right;
	private BinaryOperator<Double> supremum = (x, y) -> (Double)Math.max(x, y);
	private String supremumName = "max";
	private BinaryOperator<Double> unionValueCombinator = (x, y) -> x + y;
	private String unionValueCombinatorName = "add";
	private BinaryOperator<Double> product = Goguen.PRODUCT;
	private String productName = "goguenProduct";
	
	public Table getLeft() {
		return this.left;
	}
	
	public Table getRight() {
		return this.right;
	}
	
	public BinaryOperator<Double> getSupremum(){
		return this.supremum;
	}
	
	public BinaryOperator<Double> getUnionValueCombinator(){
		return this.unionValueCombinator;
	}
	
	public BinaryOperator<Double> getProduct(){
		return this.product;
	}
	
	public BinaryOperationContract() {
		super();
	}

	protected void initFromMap(Map<String, String> args) {
		super.initFromMap(args);
		
		var sup = args.get("supremum");
		if(sup != null) {
			this.supremumName = sup;
			this.supremum = BinaryOperators.get(sup);
		}
		
		var pro = args.get("product");
		if(pro != null) {
			this.productName = pro;
			this.product = BinaryOperators.get(pro);
		}
		
		var com = args.get("unionValueCombinator");
		if(com != null) {
			this.unionValueCombinatorName = com;
			this.unionValueCombinator = BinaryOperators.get(com);
		}
	}

	@Override
	public void setTables(List<String> paths) {
		try {
			var tableReader = TableReader.open(paths.get(0));
			this.left = tableReader.read();
			
			tableReader = TableReader.open(paths.get(1));
			this.right = tableReader.read();
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public String toString() {
		return new StringBuilder(super.toString())
				.append("unionValueCombinator").append("=").append(this.unionValueCombinatorName).append(";")
				.append("product").append("=").append(this.productName).append(";")
				.append("supremum").append("=").append(this.supremumName).append(";")
				.toString();
	}
}
