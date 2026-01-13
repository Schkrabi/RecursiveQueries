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
	private final List<IStatistic> statistics = new LinkedList<IStatistic>();
	private final Table table;
	
	public Statistics(Table table) {
		this.table = table;
	}
	
	/**
	 * Refreshes all statistics
	 */
	public void gather() {
		for(IStatistic statistic : this.statistics) {
			statistic.gather(this.table);
		}
	}
	
	/**
	 * Gets all gathered statisics object
	 * @return new LinkedList
	 */
	public List<IStatistic> getAll(){
		return new LinkedList<IStatistic>(this.statistics);
	}
	
	/**
	 * Adds attribute histogram statistic for specific attribute
	 * @param <T>
	 * @param attribute observed attribute
	 */
	public <T> void addAttributeHistogram(Attribute<T> attribute) {
		if(this.getAttributeHistogram(attribute).isEmpty()) {
			if(!this.table.schema().contains(attribute)) {
				throw new RuntimeException(new AttributeNotInSchemaException(attribute, this.table.schema()));
			}
			this.statistics.add(new AttributeHistogram<T>(attribute));
		}
	}
	
	public void addAttributeHistogram(String attribute) {
		Optional<Attribute<?>> oa = this.table.schema().attributeSet().stream()
				.filter(a -> a.name.equals(attribute)).findAny();
		
		if(oa.isPresent()) {
			this.addAttributeHistogram(oa.get());
		}
	}
	
//	public Optional<AttributeHistogram<?>> getAttributeHistogram(String attribute) {
//		Optional<Attribute<?>> oa = this.table.schema().attributeSet().stream()
//				.filter(a -> a.name.equals(attribute)).findAny();
//		
//		if(oa.isPresent()) {
//			return this.getAttributeHistogram(oa.get());
//		}
//		return Optional.empty();
//	}
	
	/**
	 * Gets attribute histogram of specific attribute. Returns empty optional if the attribute is not counted.
	 * @param <T>
	 * @param attribute observed attribute
	 * @return Optional
	 */
	@SuppressWarnings("unchecked")
	public <T> Optional<AttributeHistogram<T>> getAttributeHistogram(Attribute<T> attribute) {
		Optional<IStatistic> o = 
				this.statistics.stream()
				.filter(s -> (s instanceof AttributeHistogram) && ((AttributeHistogram<T>)s).counted.equals(attribute))
				.findAny();
		if(o.isPresent()) {
			return Optional.of((AttributeHistogram<T>)o.get());
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
		Optional<IStatistic> o = 
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
		Optional<IStatistic> o = 
				this.statistics.stream()
					.filter(x -> x instanceof RankHistogram && ((RankHistogram)x).getSlices().size() == slices)
					.findAny();
		
		if(o.isPresent()) {
			return Optional.of((RankHistogram)o.get());
		}
		return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public <T extends IStatistic> Optional<T> getStatisticByFilter(Predicate<IStatistic> predicate){
		Optional<IStatistic> o = 
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
	 * @param <T>
	 * @param attribute
	 * @param slices
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes"})
	public <T> Optional<SlicedHistogram<T>> getSlicedHistogram(Attribute<T> attribute, int slices){
		Optional<IStatistic> o = 
				this.statistics.stream()
					.filter(x -> x instanceof SlicedHistogram
							&& ((SlicedHistogram)x).attribute.equals(attribute)
							&& ((SlicedHistogram)x).getSlices().size() == slices)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((SlicedHistogram<T>)o.get());
		}
		return Optional.empty();
	}
	
	/**
	 * Adds sliced histogram
	 * @param <T>
	 * @param attribute
	 * @param slices
	 */
	public <T> void addSlicedHistogram(Attribute<T> attribute, int slices) {
		if(this.getSlicedHistogram(attribute, slices).isEmpty()) {
			this.statistics.add(new SlicedHistogram<T>(attribute, slices));
		}
	}
	
	public Optional<Size> getSize(){
		Optional<IStatistic> o = 
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
	
	@SuppressWarnings("unchecked")
	public <T extends Number> Optional<SampledHistogram<T>> getSampledHistogram(Attribute<T> attribute, double sampleSize){
		Optional<IStatistic> o =
				this.statistics.stream()
					.filter(x -> (x instanceof SampledHistogram)
							&& 	((SampledHistogram<?>)x).observed.equals(attribute)
							&&	((SampledHistogram<?>)x).sampleSize == sampleSize)
					.findAny();
		if(o.isPresent()) {
			return Optional.of((SampledHistogram<T>)o.get());
		}
		return Optional.empty();
	}
	
	public <T extends Number> void addSampledHistogram(Attribute<T> attribute, double sampleSize) {
		if(this.getSampledHistogram(attribute, sampleSize).isEmpty()) {
			this.statistics.add(new SampledHistogram<T>(attribute, sampleSize));
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Number> Optional<EquinominalHistogram<T>> getEquinominalHistogram(Attribute<T> a, int n){
		var hist = this.getStatisticByFilter(s -> (s instanceof EquinominalHistogram)
				&& ((EquinominalHistogram<?>)(s)).observed.equals(a)
				&& ((EquinominalHistogram<?>)(s)).n == n);
		if(hist.isPresent()) {
			return Optional.of((EquinominalHistogram<T>)hist.get());
		}
		return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Number> Optional<EquidistantHistogram<T>> getEquidistantHistogram(Attribute<T> a, int n){
		var hist = this.getStatisticByFilter(s -> (s instanceof EquidistantHistogram)
				&& ((EquidistantHistogram<?>)(s)).observed.equals(a)
				&& ((EquidistantHistogram<?>)(s)).n == n);
		if(hist.isPresent()) {
			return Optional.of((EquidistantHistogram<T>)hist.get());
		}
		return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Number> Optional<DataSlicedHistogram<T>> getDataSlicedHistogram(Attribute<T> a, int n){
		var hist = this.getStatisticByFilter(s -> s instanceof DataSlicedHistogram 
				&& ((DataSlicedHistogram<?>)s).observed.equals(a)
				&& ((DataSlicedHistogram<?>)s).n == n);
		if(hist.isPresent()) {
			return Optional.of((DataSlicedHistogram<T>) hist.get()); 
		}
		return Optional.empty();
	}
	
	public <T extends Number> void addEquinominalHistogram(Attribute<T> a, int n) {
		if(this.getEquinominalHistogram(a, n).isEmpty()) {
			this.statistics.add(new EquinominalHistogram<T>(a, n));
		}
	}
	
	public <T extends Number> void addEquidistantHistogram(Attribute<T> a, int n) {
		if(this.getEquidistantHistogram(a, n).isEmpty()) {
			this.statistics.add(new EquidistantHistogram<T>(a, n));
		}
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Number> Optional<MostCommonValues<T>> getMostCommonValues(Attribute<T> a) {
		var mcv = this.getStatisticByFilter(s -> (s instanceof MostCommonValues)
				&& ((MostCommonValues<?>)(s)).observed.equals(a));
		if(mcv.isPresent()) {
			return Optional.of((MostCommonValues<T>)mcv.get());
		}
		return Optional.empty();
	}
	
	public <T extends Number> void addMostCommonValues(Attribute<T> a) {
		if(this.getMostCommonValues(a).isEmpty()) {
			this.statistics.add(new MostCommonValues<T>(a));
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
