package rq.common.statistic;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import rq.common.exceptions.AttributeNotInSchemaException;
import rq.common.interfaces.Table;
import rq.common.statistic.SlicedStatistic.RankInterval;
import rq.common.table.Attribute;

/**
 * Holds statistics for a specific table
 */
public class Statistics {
	private final List<AbstractStatistic> statistics = new LinkedList<AbstractStatistic>();
	private final Table table;
	
	public Statistics(Table table) {
		this.table = table;
	}
	
	/**
	 * Refreshes all statistics
	 */
	public void gather() {
		for(AbstractStatistic statistic : this.statistics) {
			statistic.gather(this.table);
		}
	}
	
	/**
	 * Gets all gathered statisics object
	 * @return new LinkedList
	 */
	public List<AbstractStatistic> getAll(){
		return new LinkedList<AbstractStatistic>(this.statistics);
	}
	
	/**
	 * Adds attribute histogram statistic for specific attribute
	 * @param attribute observed attribute
	 */
	public void addAttributeHistogram(Attribute attribute) {
		if(this.getAttributeHistogram(attribute).isEmpty()) {
			if(!this.table.schema().contains(attribute)) {
				throw new RuntimeException(new AttributeNotInSchemaException(attribute, this.table.schema()));
			}
			this.statistics.add(new AttributeHistogram(attribute));
		}
	}
	
	public void addAttributeHistogram(String attribute) {
		Optional<Attribute> oa = this.table.schema().attributeSet().stream()
				.filter(a -> a.name.equals(attribute)).findAny();
		
		if(oa.isPresent()) {
			this.addAttributeHistogram(oa.get());
		}
	}
	
	public Optional<AttributeHistogram> getAttributeHistogram(String attribute) {
		Optional<Attribute> oa = this.table.schema().attributeSet().stream()
				.filter(a -> a.name.equals(attribute)).findAny();
		
		if(oa.isPresent()) {
			return this.getAttributeHistogram(oa.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Gets attribute histogram of specific attribute. Returns empty optional if the attribute is not counted.
	 * @param attribute observed attribute
	 * @return Optional
	 */
	public Optional<AttributeHistogram> getAttributeHistogram(Attribute attribute) {
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
				.filter(s -> (s instanceof AttributeHistogram) && ((AttributeHistogram)s).counted.equals(attribute))
				.findAny();
		if(o.isPresent()) {
			return Optional.of((AttributeHistogram)o.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Adds value count of all attributes to this statistics
	 */
	public void addValueCounts() {
		if(this.getValueCounts().isEmpty()) {
			this.statistics.add(new ValueCount());
		}
	}
	
	/**
	 * Gets value count
	 * @return value count statistic
	 */
	public Optional<ValueCount> getValueCounts(){
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
				.filter(s -> (s instanceof ValueCount))
				.findAny();
		if(o.isPresent()) {
			return Optional.of((ValueCount)o.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Gets rank histogram if it is observed with given number of slices
	 * @param slices number of slices
	 * @return Optional of Rank Histogram statistics
	 */
	public Optional<RankHistogram> getRankHistogram(int slices) {
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
					.filter(x -> x instanceof RankHistogram && ((RankHistogram)x).getSlices().size() == slices)
					.findAny();
		
		if(o.isPresent()) {
			return Optional.of((RankHistogram)o.get());
		}
		return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public <T extends AbstractStatistic> Optional<T> getStatisticByFilter(Predicate<AbstractStatistic> predicate){
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
					.filter(predicate)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((T)o.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Adds rank histogram with given number of slices
	 * @param slices
	 */
	public void addRankHistogram(int slices){
		if(this.getRankHistogram(slices).isEmpty()) {
			this.statistics.add(new RankHistogram(slices));
		}
	}
	
	public Optional<RankHistogram> getRankHistogram(Set<RankInterval> slices){
		return this.getStatisticByFilter(s -> (s instanceof RankHistogram) && ((RankHistogram)s).getSlices().equals(slices));
	}
	
	public void addRankHistogram(Set<RankInterval> slices) {
		if(this.getRankHistogram(slices).isEmpty()) {
			this.statistics.add(new RankHistogram(slices));
		}
	}
	
	/**
	 * Finds sliced histogram if exists
	 * @param attribute
	 * @param slices
	 * @return
	 */
	public Optional<SlicedHistogram> getSlicedHistogram(Attribute attribute, int slices){
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
					.filter(x -> x instanceof SlicedHistogram 
							&& ((SlicedHistogram)x).attribute.equals(attribute)
							&& ((SlicedHistogram)x).getSlices().size() == slices)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((SlicedHistogram)o.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Adds sliced histogram
	 * @param attribute
	 * @param slices
	 */
	public void addSlicedHistogram(Attribute attribute, int slices) {
		if(this.getSlicedHistogram(attribute, slices).isEmpty()) {
			this.statistics.add(new SlicedHistogram(attribute, slices));
		}
	}
	
	public Optional<Size> getSize(){
		Optional<AbstractStatistic> o = 
				this.statistics.stream()
					.filter(x -> x instanceof Size)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((Size)o.get());
		}
		return Optional.empty();
	}
	
	public void addSize() {
		if(this.getSize().isEmpty()) {
			this.statistics.add(new Size());
		}
	}
	
	public Optional<SampledHistogram> getSampledHistogram(Attribute attribute, double sampleSize){
		Optional<AbstractStatistic> o =
				this.statistics.stream()
					.filter(x -> (x instanceof SampledHistogram)
							&& 	((SampledHistogram)x).observed.equals(attribute)
							&&	((SampledHistogram)x).sampleSize == sampleSize)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((SampledHistogram)o.get());
		}
		return Optional.empty();
	}
	
	public void addSampledHistogram(Attribute attribute, double sampleSize) {
		if(this.getSampledHistogram(attribute, sampleSize).isEmpty()) {
			this.statistics.add(new SampledHistogram(attribute, sampleSize));
		}
	}
	
	public Optional<EquinominalHistogram> getEquinominalHistogram(Attribute a, int n){
		var hist = this.getStatisticByFilter(s -> (s instanceof EquinominalHistogram)
				&& ((EquinominalHistogram)(s)).observed.equals(a)
				&& ((EquinominalHistogram)(s)).n == n);
		if(hist.isPresent()) {
			return Optional.of((EquinominalHistogram)hist.get());
		}
		return Optional.empty();
	}
	
	public Optional<EquidistantHistogram> getEquidistantHistogram(Attribute a, int n){
		var hist = this.getStatisticByFilter(s -> (s instanceof EquidistantHistogram)
				&& ((EquidistantHistogram)(s)).observed.equals(a)
				&& ((EquidistantHistogram)(s)).n == n);
		if(hist.isPresent()) {
			return Optional.of((EquidistantHistogram)hist.get());
		}
		return Optional.empty();
	}
	
	public Optional<DataSlicedHistogram> getDataSlicedHistogram(Attribute a, int n){
		var hist = this.getStatisticByFilter(s -> s instanceof DataSlicedHistogram 
				&& ((DataSlicedHistogram)s).observed.equals(a)
				&& ((DataSlicedHistogram)s).n == n);
		if(hist.isPresent()) {
			return Optional.of((DataSlicedHistogram) hist.get()); 
		}
		return Optional.empty();
	}
	
	public void addEquinominalHistogram(Attribute a, int n) {
		if(this.getEquinominalHistogram(a, n).isEmpty()) {
			this.statistics.add(new EquinominalHistogram(a, n));
		}
	}
	
	public void addEquidistantHistogram(Attribute a, int n) {
		if(this.getEquidistantHistogram(a, n).isEmpty()) {
			this.statistics.add(new EquidistantHistogram(a, n));
		}
	}
	
	@Override
	public String toString() {
		return this.statistics.stream()
				.map(s -> s.toString())
				.reduce((x, y) -> new StringBuilder()
										.append(x)
										.append(" ")
										.append(y)
										.toString())
				.get();
	}
}
