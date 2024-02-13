package rq.estimations.main;

import java.util.function.BiFunction;

import rq.common.statistic.RankHistogram;
import rq.common.table.Attribute;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;

public class Measurement {
	
	public final Attribute attribute;
	public final double value;
	public final BiFunction<Object, Object, Double> similarity;
	public final Table data;
	public final EstimationSetupContract contract;
	
	private final String similarityName;
	private final String estimationName;
	
	public final BiFunction<Selection, EstimationSetupContract, RankHistogram> estimateProvider;
	
	private final String resultHeader = "accuracy, inaccuracy, estimation, attribute, value, similarity, estimated data, actual data, contract";

	public Measurement(
			Attribute attribute,
			double value,
			Table data,
			EstimationSetupContract contract,
			String similarityName,
			String estimationName) {
		this.attribute = attribute;
		this.value = value;
		this.data = data;
		this.contract = contract;
		this.similarityName = similarityName;
		this.similarity = SimilarityProvider.gets(similarityName);
		this.estimationName = estimationName;
		this.estimateProvider = EstimationProviders.parse(estimationName);
	}

	public String measure() {		
		Selection selection = new Selection(
								this.data,
								new Similar(this.attribute, new Constant<Double>(value), this.similarity));
		
		if(this.contract.getEquinominal()) {
			this.data.getStatistics()
				.addEquinominalHistogram(this.attribute, contract.getDomainSamples());
		}
		else {
			this.data.getStatistics()
				.addEquidistantHistogram(attribute, contract.getDomainSamples());
		}
		this.data.getStatistics().addSampledHistogram(attribute, contract.getDomainSampleSize());
		this.data.getStatistics().gather();
		
		RankHistogram estimate = this.estimateProvider.apply(selection, this.contract);
		
		Table selected = selection.eval();
		selected.getStatistics().addRankHistogram(estimate.getSlices());
		
		selected.getStatistics().gather();
		
		RankHistogram actual = selected.getStatistics().getRankHistogram(estimate.getSlices()).get();
		
		return this.outputResult(estimate, actual);
	}
	
	private String outputResult(
			RankHistogram estimated,
			RankHistogram actual) {
		return new StringBuilder()
				.append(this.resultHeader).append("\n")
				.append(this.accuracy(estimated, actual, data.size())).append(",")
				.append(this.inaccuracy(estimated, actual, data.size())).append(",")
				.append(this.estimationName).append(",")
				.append(this.attribute.name).append(",")
				.append(this.value).append(",")
				.append(this.similarityName).append(",")
				.append("\"").append(estimated.get().toString()).append("\",")
				.append("\"").append(actual.get().toString()).append("\",")
				.append("\"").append(this.contract.toString()).append("\"")
				.toString();
	}
	
	/**
	 * Funkce vraci "presnost" odhadu vzhledem ke skutecne ziskanym vysledkum.
	 * 
	 * Presnost je vyjadrena jako pomer vsech spravne urcenych radku s danym rankem
	 * ku poctu vsech prirazeni.
	 * 
	 * Priklady:
	 * 
	 * (1) Pokud provedeny dotaz ukazuje, ze vysledek obsahuje 10 radku s rankem
	 * z intervalu (0.5, 0.75] a odhad ukazuje 8, znamena to, ze spravne bylo urceno
	 * jen 8 radku. 
	 * 
	 * (2) Pokud provedeny dotaz ukazuje, ze vysledek obsahuje 10 radku s rankem
	 * z intervalu (0.5, 0.75] a odhad ukazuje 12, znamena to, ze spravne bylo urceno
	 * jen 10 radku.
	 * 
	 * Mezi spravne urcene ranky radku se pocitaji i ty, ktere maji rank 0
	 * a nejsou soucasti histogramu.
	 * 
	 * @param estimate -- histogram, ktery vznikl odhadem
	 * @param measurement -- histogram, ktery vznikl provedenim dotazu a predstavuje skutecne pocty radku
	 * @param dataSize -- celkovy pocet radku, ze kterych byl provaden odhad
	 * @return hodnotu od 0.0 do 1.0 predstavujici presnost odhadu
	 */
	public double accuracy(RankHistogram estimate, RankHistogram measurement, int dataSize) {
		int correct = 0;
		for (var interval : measurement.getSlices()) {
			correct += Math.min(measurement.get(interval), estimate.get(interval));
		}
		
		// pocita radky, ktere maji rank 0 a nejsou tedy soucasti histogramu
		int zeroRankEst = (int) (dataSize - estimate.tableSize());
		int zeroRankMeasured = (int) (dataSize - measurement.tableSize());
		correct += Math.min(zeroRankEst, zeroRankMeasured);
		
		return correct / (double) Math.max(dataSize, estimate.tableSize());
	}
	
	/**
	 * Funkce vraci "nepresnost" odhadu vzhledem ke skutecne ziskanym vysledkum.
	 * 
	 * Nepresnost je vyjadrena jako pomer spatne urcenych radku s danym rankem
	 * ku poctu vsech radku, k nimz byl rank prirazovan.
	 * 
	 * Priklady:
	 * (1) Pokud provedeny dotaz ukazuje, ze vysledek obsahuje 10 radku s rankem
	 * z intervalu (0.5, 0.75] a odhad ukazuje 8, znamena to, ze spatne bylo urceny 2 radky.
	 *  
	 * (2) Pokud provedeny dotaz ukazuje, ze vysledek obsahuje 10 radku s rankem
	 * z intervalu (0.5, 0.75] a odhad ukazuje 15, znamena to, ze spatne bylo urceno
	 * 5 radku.
	 * 
	 * Mezi spatne urcene ranky radku se pocitaji i ty, ktere maji rank 0
	 * a nejsou soucasti histogramu.
	 * 
	 * @param estimate -- histogram, ktery vznikl odhadem
	 * @param measurement -- histogram, ktery vznikl provedenim dotazu a predstavuje skutecne pocty radku
	 * @param dataSize -- celkovy pocet radku, ze kterych byl provaden odhad
	 * @return hodnota z intervalu od 0.0 do nekonecna, ktera predstavuje nepresnost odhadu
	 */
	public double inaccuracy(RankHistogram estimate, RankHistogram measurement, int dataSize) {
	
		int err = 0;
		for (var interval : measurement.getSlices()) {
			err += Math.abs(measurement.get(interval) - estimate.get(interval));
		}
		
		int zeroRankErr = (int) Math.abs((dataSize - measurement.tableSize()) - (dataSize - estimate.tableSize()));
		err += zeroRankErr;
		
		return err / (double) dataSize;
	}
}
