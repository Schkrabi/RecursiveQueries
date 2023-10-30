/**
 * 
 */
package rq.common.operators;

import java.util.function.BiFunction;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.exceptions.ComparisonDomainMismatchException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.TabularExpression;
import rq.common.latices.LaticeFactory;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.table.Record;
import rq.common.table.Schema;
import rq.common.interfaces.Table;

/**
 * Represents similarity restriction operation on table
 * @author Mgr. R.Skrabal
 *
 * Similary is not type checked!
 */
public class SimilarityRestriction implements TabularExpression{
	private final TabularExpression argument;
	private final Attribute attribute1;
	private final Attribute attribute2;
	private final BiFunction<Double, Double, Double> product;
	private final BiFunction<Object, Object, Double> similarity;
	private final BiFunction<Schema, Integer, Table> tableSupplier;
	
	private SimilarityRestriction(
			TabularExpression argument, 
			Attribute attribute1, 
			Attribute attribute2, 
			BiFunction<Double, Double, Double> product, 
			BiFunction<Object, Object, Double> similarity,
			BiFunction<Schema, Integer, Table> tableSupplier) {
		this.argument = argument;
		this.attribute1 = attribute1;
		this.attribute2 = attribute2;
		this.product = product;
		this.similarity = similarity;
		this.tableSupplier = tableSupplier;
	}
	
	public static SimilarityRestriction factory(
			TabularExpression argument, 
			Attribute attribute1, 
			Attribute attribute2, 
			BiFunction<Object, Object, Double> similarity)
		throws AttributeNotInSchemaException, ComparisonDomainMismatchException {
		return SimilarityRestriction.factory(
				argument,
				attribute1,
				attribute2,
				LaticeFactory.instance().getProduct(),
				similarity);
	}
	
	public static SimilarityRestriction factory(
			TabularExpression argument, 
			Attribute attribute1, 
			Attribute attribute2, 
			BiFunction<Double, Double, Double> product, 
			BiFunction<Object, Object, Double> similarity)
		throws AttributeNotInSchemaException, ComparisonDomainMismatchException {
		return SimilarityRestriction.factory(
				argument, 
				attribute1, 
				attribute2, 
				product, 
				similarity,
				(Schema s, Integer count) -> new MemoryTable(s));
	}
	
	/**
	 * Factory method
	 * @param argument over which restriction is performed
	 * @param attribute1 
	 * @param attribute2 
	 * @param product product to aggregate ranks
	 * @return table instance
	 * @throws AttributeNotInSchemaException if either attribute is not in table schema
	 * @throws ComparisonDomainMismatchException 
	 */
	public static SimilarityRestriction factory(
			TabularExpression argument, 
			Attribute attribute1, 
			Attribute attribute2, 
			BiFunction<Double, Double, Double> product, 
			BiFunction<Object, Object, Double> similarity,
			BiFunction<Schema, Integer, Table> tableSupplier)
		throws AttributeNotInSchemaException, ComparisonDomainMismatchException {
		if(!attribute1.domain.equals(attribute2.domain)) {
			throw new ComparisonDomainMismatchException(attribute1, attribute2);
		}
		if(argument.schema().attributeIndex(attribute1).isEmpty()) {
			throw new AttributeNotInSchemaException(attribute1, argument.schema());
		}
		if(argument.schema().attributeIndex(attribute2).isEmpty()) {
			throw new AttributeNotInSchemaException(attribute2, argument.schema());
		}
		return new SimilarityRestriction(
				argument,
				attribute1,
				attribute2,
				product,
				similarity,
				tableSupplier);
	}
	
	/**
	 * Applies the operator
	 * @return
	 */
	public Table eval() {
		Table table = this.argument.eval();
		Table ret = this.tableSupplier.apply(table.schema(), table.size());
		table.stream()
				.map((Record r) -> {
					try {
						return new Record(r, this.product.apply(r.rank,
								this.similarity.apply(r.get(this.attribute1), r.get(this.attribute2))));
					} catch (AttributeNotInSchemaException e) {
						// Unlikely
						throw new RuntimeException(e);
					}
				})
				.forEach(r -> {
					try {
						ret.insert(r);
					} catch (TableRecordSchemaMismatch e) {
						// Unlikely
						throw new RuntimeException(e);
					}
				});
		return ret;
	}
	
	@Override
	public Schema schema() {
		return this.argument.schema();
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
				.append("SELECT ")
				.append(this.argument.toString())
				.append(" WHERE ")
				.append(this.attribute1.toString())
				.append(" ~ ")
				.append(this.attribute2.toString())
				.toString();
	}
}
