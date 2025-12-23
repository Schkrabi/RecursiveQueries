package rq.files.contracts;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.squareup.moshi.JsonAdapter;
import com.squareup.moshi.Moshi;

import rq.common.table.Attribute;
import rq.common.util.ISerilazeable;

/**Contract for experiment arguments*/
public class EstimationExperimentContract
	implements ISerilazeable<EstimationExperimentContract> {

	public EstimationExperimentContract() {}
	public EstimationExperimentContract(
			String file,
			String workingDirectory,
			int slices,
			int probes,
			long seed,
			Collection<AttributeContract> atts) {
		this.file = file;
		this.workingDirectory = workingDirectory;
		this.slices = slices;
		this.probes = probes;
		this.seed = seed;
		this.attributes = new ArrayList<AttributeContract>(atts);
	}
	
	public String file;
	public String workingDirectory;
	/** Number of slices for estimation and query result*/
	public int slices;
	/** Number of probes for stochastic estimations (unused) */
	public int probes;
	/**Seed for random generation*/
	public long seed;
	public String queryValuesGenerationStrategy;
	public List<AttributeContract> attributes = new ArrayList<AttributeContract>();
	
	@Override
	public boolean equals(Object other) {
		if(other instanceof EstimationExperimentContract cnt) {
			return this.file.equals(cnt.file)
					&& this.workingDirectory.equals(cnt.workingDirectory)
					&& this.slices == cnt.slices
					&& this.probes == cnt.probes
					&& this.attributes.equals(cnt.attributes);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return new StringBuilder()
				.append(this.file.hashCode())
				.append(this.workingDirectory.hashCode())
				.append(Integer.hashCode(this.slices))
				.append(Integer.hashCode(this.probes))
				.append(this.attributes.hashCode())
				.toString().hashCode();
	}
	
	public QueryGenerationStrategy getQueryGenerationStrategy() {
		try {
			return QueryGenerationStrategy.valueOf(this.queryValuesGenerationStrategy);
		} catch(IllegalArgumentException | NullPointerException e) {
			return QueryGenerationStrategy.Values;
		}
	}
	
	public static class AttributeContract {
		public AttributeContract() {}
		public AttributeContract(
				String name,
				String typeName,
				double histSampleSize,
				int consideredValues,
				double paretValue,
				int intervals,
				double similarUntil,
				Collection<Double> restrictionQueryArgs) {
			this.name = name;
			this.typeName = typeName;
			this.histSampleSize = histSampleSize;
			this.consideredValues = consideredValues;
			this.paretValue = paretValue;
			this.intervals = intervals;
			this.similarUntil = similarUntil;
			this.restrictionQryArgs = new ArrayList<Double>(restrictionQueryArgs);
		}
		
		/**Name of the attribute*/
		public String name;
		/**Type of the attribute*/
		public String typeName;
		/** Sample size for sampled histogram */
		public double histSampleSize;
		/** Number of considered values for paret based estimation */
		public int consideredValues;
		/** Ratio for parametrized paret estimations (unused) */
		public double paretValue;
		/** Number of intervals for interval based estimations */
		public int intervals;
		/** Defines the similarity */
		public double similarUntil;
		/** List of restriction query arguments */
		public List<Double> restrictionQryArgs = new ArrayList<Double>();
		
		@Override
		public boolean equals(Object other) {
			if(other instanceof AttributeContract cnt) {
				return this.name.equals(cnt.name)
						&& this.typeName.equals(cnt.typeName)
						&& this.histSampleSize == cnt.histSampleSize
						&& this.consideredValues == cnt.consideredValues
						&& this.paretValue == cnt.paretValue
						&& this.intervals == cnt.intervals
						&& this.similarUntil == cnt.similarUntil
						&& this.restrictionQryArgs.equals(cnt.restrictionQryArgs);
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return new StringBuilder()
					.append(this.name.hashCode())
					.append(this.typeName.hashCode())
					.append(Double.hashCode(this.histSampleSize))
					.append(Integer.hashCode(consideredValues))
					.append(Double.hashCode(this.paretValue))
					.append(Integer.hashCode(this.intervals))
					.append(Double.hashCode(this.similarUntil))
					.append(this.restrictionQryArgs.hashCode())
					.toString().hashCode();
		}
		
		/**Gets type of the attribute*/
		public Class<?> getType(){
			if(this.typeName == null || this.typeName == "") {
				return null;
			}
			try {
				return Class.forName(this.typeName);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
		
		/**Gets the atribute*/
		public Attribute getAttribute() {
			return new Attribute(this.name, this.getType());
		}
	}

	private static Moshi moshi = null;
	private static JsonAdapter<EstimationExperimentContract> adapter = null;
	
	protected static Moshi getMoshi() {
		if(moshi == null) {
			moshi = new Moshi.Builder().build();
		}
		return moshi;
	}
	
	protected static JsonAdapter<EstimationExperimentContract> getAdapter(){
		if(adapter == null) {
			adapter = EstimationExperimentContract.getMoshi().adapter(EstimationExperimentContract.class);
		}
		return adapter;
	}
	
	@Override
	public String serialize() {
		return EstimationExperimentContract.getAdapter().toJson(this);
	}
	
	public static EstimationExperimentContract deserialize(String ser) {
		try {
			return EstimationExperimentContract.getAdapter().fromJson(ser);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
