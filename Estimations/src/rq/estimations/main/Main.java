package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.estimations.AbstractSelectionEstimation;
import rq.common.estimations.IntervalEstimation;
import rq.common.estimations.Nominal;
import rq.common.estimations.Nominal_stochastic;
import rq.common.estimations.Numerical;
import rq.common.estimations.Numerical_domainPruning;
import rq.common.estimations.Numerical_stochastic;
import rq.common.estimations.Numerical_stochasticAndDomainPruning;
import rq.common.estimations.PostProcessEstimateWithFuzzyTable;
import rq.common.estimations.ProbeableEstimation;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Equals;
import rq.common.restrictions.Similar;
import rq.common.similarities.LinearSimilarity;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.statistic.Statistics;
import rq.common.table.Attribute;
import rq.common.table.MemoryTable;
import rq.common.types.Str10;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.io.LazyTable;
import rq.files.io.TableReader;
import data.Toloker;

public class Main {

	public Main() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws IOException, CsvValidationException, ClassNotFoundException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch {
		final int SLICES = 3;
		final double DOMAIN_SAMPLE_SIZE = 0.05d;
		final int STOCHASTIC_SAMPLES = 6;
		final BiFunction<Object, Object, Double> SIMILARITY = LinearSimilarity.doubleSimilarityUntil(0.2);
		final int probes = 10;
		
		//Args: [fileName] [attributeName] [conditionConst]
		//1. Load data
		Path filepath = Path.of(args[0]);
		LazyTable data = LazyTable.open(filepath);
		
		//2. Give data random ranks
		Random rand = new Random();
		Table rankedData = new MemoryTable(data.schema());
		
		//RankHistogram.setEquinominal();
		
		rq.common.table.Record record = data.next();
		while(record != null) {
//			double rank = 1.0d - rand.nextDouble();
			double rank = 1.0d;
			rq.common.table.Record r = new rq.common.table.Record(record, rank);
			rankedData.insert(r);
			
			record = data.next();
		}
		
		//3. Give data statistics
		Attribute a = rankedData.schema().stream().filter(x -> x.name.equals(args[1])).findAny().get();
		
		Statistics stats = rankedData.getStatistics();
		stats.addAttributeHistogram(a);
		stats.addSampledHistogram(a, DOMAIN_SAMPLE_SIZE);
		stats.addSlicedHistogram(a, SLICES);
		stats.addSize();
		stats.addRankHistogram(SLICES);
		stats.addEquinominalHistogram(a, 20);
		//stats.addEquidistantHistogram(a, 20);
		
		//4. Gather statistics
		stats.gather();
		
		//5. Print statistics¨
		System.out.println(stats.toString());		
		
		//5.1. Define restriction 
		//TODO type of the constant!
		Selection selection = new Selection(rankedData,
				//new Equals(a, new Constant<Str10>(Str10.factory(args[2]))));
				//new Similar(a, new Constant<Str10>(Str10.factory(args[2])), Toloker.educationSimilarity));
				new Similar(a, new Constant<Double>(Double.parseDouble(args[2])), SIMILARITY));
		
		
		//6. Make estimation
//		GeneralHistogramCrispCondOnFuzzyTableEstimation estimation1 = 
//				new GeneralHistogramCrispCondOnFuzzyTableEstimation(selection, rankedData, SLICES);
//		RankHistogram est1 = estimation1.estimate();
//		
//		GeneralShistogramCrispConstOnFuzzyTableEstimation estimation2 =
//				new GeneralShistogramCrispConstOnFuzzyTableEstimation(selection, rankedData, SLICES);
//		RankHistogram est2 = estimation2.estimate();
		
//		AbstractSelectionEstimation estimation3 =
//				new Nominal(
//						selection, 
//						rankedData, 
//						SLICES, 
//						2, 
//						Toloker.educations, 
//						Toloker.educationSimilarity);
//		RankHistogram est3 = estimation3.estimate(2);
//		est3 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est3, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
//		
//		AbstractSelectionEstimation estimation4 =
//				new Nominal(
//						selection, 
//						rankedData, 
//						SLICES, 
//						2, 
//						Toloker.educations, 
//						Toloker.educationSimilarity);
//		RankHistogram est4 = estimation4.estimate();
//		est4 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est4, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
//		
//		AbstractSelectionEstimation estimation6 =
//				new Nominal_stochastic(
//						selection, 
//						rankedData, 
//						SLICES, 
//						1, 
//						Toloker.educations, 
//						Toloker.educationSimilarity,
//						2);
//		RankHistogram est6 = estimation6.estimate();
//		est6 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est6, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		AbstractSelectionEstimation estimation7 =
				new Numerical(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE);
		RankHistogram est7 = estimation7.estimate();
//		est7 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est7, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		ProbeableEstimation estimation8 =
				new Numerical(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE);
		estimation8.setProbes(probes);
		RankHistogram est8 = estimation8.estimate();
//		est8 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est8, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		AbstractSelectionEstimation estimation9 =
				new Numerical_domainPruning(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE);
		RankHistogram est9 = estimation9.estimate();
//		est9 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est9, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		ProbeableEstimation estimation10 =
				new Numerical_domainPruning(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE);
		estimation10.setProbes(probes);
		RankHistogram est10 = estimation10.estimate();
//		est10 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est10, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		AbstractSelectionEstimation estimation11 =
				new Numerical_stochastic(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE,
						STOCHASTIC_SAMPLES);
		RankHistogram est11 = estimation11.estimate();
//		est11 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est11, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		ProbeableEstimation estimation12 =
				new Numerical_stochastic(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE,
						STOCHASTIC_SAMPLES);
		estimation12.setProbes(probes);
		RankHistogram est12 = estimation12.estimate();
//		est12 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est12, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		AbstractSelectionEstimation estimation13 =
				new Numerical_stochasticAndDomainPruning(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE,
						STOCHASTIC_SAMPLES);
		RankHistogram est13 = estimation13.estimate();
//		est13 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est13, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		ProbeableEstimation estimation14 =
				new Numerical_stochasticAndDomainPruning(
						selection, 
						SLICES,
						DOMAIN_SAMPLE_SIZE,
						STOCHASTIC_SAMPLES);
		estimation14.setProbes(probes);
		RankHistogram est14 = estimation14.estimate();
//		est14 = PostProcessEstimateWithFuzzyTable.recalculate(
//				est14, 
//				rankedData.getStatistics().getRankHistogram(SLICES).get(), 
//				selection.product);
		
		AbstractSelectionEstimation estimation15 =
				new IntervalEstimation(
						selection,
						SLICES,
						20);
		RankHistogram est15 = estimation15.estimate();
		
		//7. Print estimation
//		System.out.println("Estimation (hist + rankHist): " + est1.toString());
//		System.out.println("Estimation (sliced hist): " + est2.toString());
//		System.out.println("Estimation (nominal values probes): " + est3.toString());
//		System.out.println("Estimation (nominal values probab): " + est4.toString());
//		System.out.println("Estimation (nom probes stochastic): " + est6.toString());
		System.out.println("Estimation (numeral basic): " + est7.toString());
		System.out.println("Estimation (number probes): " + est8.toString());
		System.out.println("Estimation (num dom prune): " + est9.toString());
		System.out.println("Estimation (num prunNprob): " + est10.toString());
		System.out.println("Estimation (num stochasti): " + est11.toString());
		System.out.println("Estimation (num probNstoc): " + est12.toString());
		System.out.println("Estimation (num stocNprun): " + est13.toString());
		System.out.println("Estimation (num proStoPru): " + est14.toString());
		System.out.println("Estimation (nume interval): " + est15.toString());
		
		//8. Execute the restriction
		Table result = selection.eval();		
		
		//9. Gather statistics over restriction
		Statistics rsltStats = result.getStatistics();
		rsltStats.addRankHistogram(est7.getSlices());
		rsltStats.gather();
		
		//10. Print restriction statistics
		System.out.println("Result " + rsltStats.toString());
		
//		SampledHistogram hist = stats.getSampledHistogram(a, DOMAIN_SAMPLE_SIZE).get();
//				
//		for(double value : hist.getHistogram().keySet()) {
//			List<Double> ranks1 = new ArrayList<Double>();
//			for(double d = 0d; d < 1.d; d += DOMAIN_SAMPLE_SIZE) {
//				double sim = SIMILARITY.apply(value, d);
//				if(sim > 0d) {
//					ranks1.add(d);
//				}
//			}
//			ranks1 = ranks1.stream().sorted().collect(Collectors.toList());
//			
//			List<Double> ranks2 = 
//					//((HistNumCondCrispTab_domPrune)estimation9).nonZeroRanks(value);
//					((HistNumCondCrispTab_domPrune)estimation9).nonZeroSimilarSamples(value).stream()
//					.sorted()
//					.collect(Collectors.toList());
//			
//			if(!ranks1.equals(ranks2)) {
//				System.out.println(value);
//				System.out.println(ranks1);
//				System.out.println(ranks1.stream().map(x -> SIMILARITY.apply(x, value)).collect(Collectors.toList()));
//				System.out.println(ranks2);
//				System.out.println(ranks2.stream().map(x -> SIMILARITY.apply(x, value)).collect(Collectors.toList()));
//			}			
//		}
	}
}
