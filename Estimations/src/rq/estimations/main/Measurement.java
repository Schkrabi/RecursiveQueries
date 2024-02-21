package rq.estimations.main;

import rq.common.statistic.RankHistogram;


public class Measurement {
	
	private final EstimationProvider provider;
	
	private final String resultHeader = "accuracy, inaccuracy, estimation, estimated data, actual data, contract";

	public Measurement(
			EstimationProvider provider) {
		this.provider = provider;
	}

	public String measure() {		
		var estimate = this.provider.estimate();
		var actual = this.provider.compute();
		
		return this.outputResult(estimate, actual);
	}
	
	private String outputResult(
			RankHistogram estimated,
			RankHistogram actual) {
		return new StringBuilder()
				.append(this.resultHeader).append("\n")
				.append(this.accuracy(estimated, actual, this.provider.dataSize())).append(",")
				.append(this.inaccuracy(estimated, actual, this.provider.dataSize())).append(",")
				.append(this.provider.name()).append(",")
				.append("\"").append(estimated.get().toString()).append("\",")
				.append("\"").append(actual.get().toString()).append("\",")
				.append("\"").append(this.provider.contract().toString()).append("\"")
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
