package rq.estimations.main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.opencsv.exceptions.CsvValidationException;

import rq.common.estimations.IntervalEquidistant;
import rq.common.estimations.IntervalEquinominal;
import rq.common.estimations.Numerical;
import rq.common.estimations.Numerical_domainPruning;
import rq.common.estimations.Numerical_stochastic;
import rq.common.estimations.Numerical_stochasticAndDomainPruning;
import rq.common.estimations.ReintroduceRanks;
import rq.common.exceptions.DuplicateAttributeNameException;
import rq.common.exceptions.NotSubschemaException;
import rq.common.exceptions.OnOperatornNotApplicableToSchemaException;
import rq.common.exceptions.SchemaNotEqualException;
import rq.common.exceptions.TableRecordSchemaMismatch;
import rq.common.interfaces.Table;
import rq.common.interfaces.TabularExpression;
import rq.common.onOperators.Constant;
import rq.common.operators.Selection;
import rq.common.restrictions.Similar;
import rq.common.statistic.EquidistantHistogram;
import rq.common.statistic.EquinominalHistogram;
import rq.common.statistic.RankHistogram;
import rq.common.statistic.SampledHistogram;
import rq.common.table.Attribute;
import rq.files.exceptions.ClassNotInContextException;
import rq.files.exceptions.ColumnOrderingNotInitializedException;
import rq.files.exceptions.DuplicateHeaderWriteException;
import rq.files.io.TableReader;
import rq.files.io.TableWriter;

public class Workbench {
	
	public static void writeRankHistograms(Table data, Path dataPath, List<Integer> slices) throws IOException {
		var dataFolder = dataPath.getParent();
		var dataName = dataPath.getFileName().toString();
		var histFolder = dataFolder.resolve("hist");
		
		if(!Files.exists(histFolder)) {
			Files.createDirectories(histFolder);
		}
		
		for(var slice : slices) {
			var hist = new RankHistogram(slice);
			hist.gather(data);
			hist.writeFile(histFolder.resolve(rankHistFileName(dataName, slice)));
		}
	}
	
	public static Table query(
			Path dataPath, 
			Function<Table, TabularExpression> query, 
			String name) throws Exception{
		
		var dataFileName = dataPath.getFileName().toString();
		var dataFolder = dataPath.getParent();
		
		var rsltFolder = dataFolder.resolve(name);
		
		if(!Files.exists(rsltFolder)) {
			Files.createDirectories(rsltFolder);
		}
		
		var rsltFileName = dataFileName(dataFileName, name);
		var rsltFilePath = rsltFolder.resolve(rsltFileName);
		
		var tr = TableReader.open(dataPath);
		var data = tr.read();
		tr.close();
		var q = query.apply(data);
		var rslt = q.eval();
		
		var tw = TableWriter.open(Files.newOutputStream(rsltFilePath));
		tw.write(rslt);
		tw.close();
		
		return rslt;
	}
	
	
	public static String dataFileName(String orgFileName, String qName) {
		return new StringBuilder()
				.append(orgFileName)
				.append(".")
				.append(qName)
				.append(".csv")
				.toString();
	}
	
	public static String rankHistFileName(Path dataFileName, int slices) {
		return rankHistFileName(dataFileName.toString(), slices);
	}
	
	public static String rankHistFileName(String dataFileName, int slices) {
		return new StringBuilder()
				.append(dataFileName)
				.append(".")
				.append(slices)
				.append(".hist")
				.toString();
	}

	
	
	public static Set<rq.common.types.Str50> loadDomain_beer_style(){
		try {
			var text = Files.readString(Path.of("C:\\Users\\r.skrabal\\Documents\\Mine\\Java\\RecursiveQueries\\estimation_experiments\\Beer Reviews\\styles.csv")	);
			var rslt = Stream.of(text.split("\n")).map(s -> rq.common.types.Str50.factory(s)).collect(Collectors.toSet());
			return rslt;
		}catch(Exception e) {
			throw new RuntimeException(e);
		}
	}

	
	
	public static String histName(String dataFileName, String attName) {
		return new StringBuilder()
				.append(dataFileName).append(".")
				.append(attName)
				.append(".hist")
				.toString();
	}
	
	public static String eqdHistName(String dataFileName, String attName, int intervals) {
		return new StringBuilder()
				.append(dataFileName).append(".")
				.append(attName)
				.append(".eqd.")
				.append(intervals)
				.append(".hist")				
				.toString();
	}
	
	public static String eqnHistName(String dataFileName, String attName, int intervals) {
		return new StringBuilder()
				.append(dataFileName).append(".")
				.append(attName)
				.append(".eqn.")
				.append(intervals)
				.append(".hist")				
				.toString();
	}
	
	public static String sampledHistName(String dataFileName, String attName) {
		return new StringBuilder()
				.append(dataFileName).append(".")
				.append(attName)
				.append(".hist")
				.toString();
	}
	
	public static void estimates_numerical(
			String path, 
			Attribute attribute, 
			String qName, 
			String dataFileName,
			BiFunction<Object, Object, Double> similarity) throws Exception{
		var slices = List.of(3, 8);
		var intervals = List.of(3, 5);
		var probes = List.of(2, 0);
		var samples = List.of(5, 2);
		
		var fileNamePrefix = dataFileName + "." + attribute.name;
		
		for(var sli : slices) {
			var hist = RankHistogram.readFile(Paths.get(path, "hist", rankHistFileName(dataFileName, sli)));
			for(var i : intervals) {
				var eqd = EquidistantHistogram.readFile(Paths.get(path, "hist", eqdHistName(dataFileName, attribute.name, i)));
				eqd(sli, similarity, eqd, hist, path, fileNamePrefix);
				
				var eqn = EquinominalHistogram.readFile(Paths.get(path, "hist", eqnHistName(dataFileName, attribute.name, i)));
				eqn(sli, similarity, eqn, hist, path, fileNamePrefix);
			}
		}
		
		var reader = TableReader.open(Paths.get(path, dataFileName));
		var data = reader.read();
		reader.close();
		var selection = new Selection(data,	new Similar(attribute, new Constant<Double>(0.d), similarity));
		
		var attHist = SampledHistogram.readFile(Paths.get(path, "hist", sampledHistName(dataFileName, attribute.name)));
		
		for(var sli : slices) {
			var hist = RankHistogram.readFile(Paths.get(path, "hist", rankHistFileName(dataFileName, sli)));
			for(var probe : probes) {
				num(selection, sli, probe, attHist, hist, path, fileNamePrefix);
				numP(selection, sli, probe, attHist, hist, path, fileNamePrefix);
				
				for(var sample : samples) {
					numS(selection, sli, probe, sample, attHist, hist, path, fileNamePrefix);
					numPS(selection, sli, probe, sample, attHist, hist, path, fileNamePrefix);
				}
			}
		}
	}
	
	public static String estFileNamePrefix(String baseFileName, String attributeName) {
		return new StringBuilder()
				.append(baseFileName)
				.append(".")
				.append(attributeName)
				.toString();
	}
	
	public static void stats(List<String> estFiles, List<String> qFiles, int size, String path, String fileName) throws IOException {
		var sb = new StringBuilder();
		sb.append(statHeader).append("\n");
		
		for(var estName : estFiles) {
			for(var qName : qFiles) {
				sb.append(statLine(
						Paths.get(path, "est", estName),
						Paths.get(path, "queries", "hist", qName),
						size));
				sb.append("\n");
			}
		}
		
		Files.writeString(Paths.get(path, fileName), sb.toString());
	}
	
	public static void eqd(int slices, BiFunction<Object, Object, Double> similarity, EquidistantHistogram eqdHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = IntervalEquidistant.estimate(slices, eqdHist, similarity);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, eqdEstName(namePrefix, slices, eqdHist.n)));
	}
	
	public static String eqdEstName(String namePrefix, int slices, int n) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".eqd.")
				.append(slices)
				.append(".")
				.append(n)
				.append(".est")
				.toString();
	}
	
	public static void eqn(int slices, BiFunction<Object, Object, Double> similarity, EquinominalHistogram eqnHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = IntervalEquinominal.estimate(slices, eqnHist, similarity);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, eqnEstName(namePrefix, slices, eqnHist.n)));
	}
	
	public static String eqnEstName(String namePrefix, int slices, int n) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".eqn.")
				.append(slices)
				.append(".")
				.append(n)
				.append(".est")
				.toString();
	}
	
	public static void num(Selection selection, int slices, int probes, SampledHistogram attHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = Numerical.estimateStatic(selection, slices, attHist.sampleSize, probes, attHist);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, numEstName(namePrefix, slices, probes)));
	}
	
	public static String numEstName(String namePrefix, int slices, int probes) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".num.")
				.append(slices)
				.append(".")
				.append(probes)
				.append(".est")
				.toString();
	}
	
	public static void numP(Selection selection, int slices, int probes, SampledHistogram attHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = Numerical_domainPruning.estimateStatic(selection, slices, attHist.sampleSize, probes, attHist);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, numPEstName(namePrefix, slices, probes)));
	}
	
	public static String numPEstName(String namePrefix, int slices, int probes) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".numP.")
				.append(slices)
				.append(".")
				.append(probes)
				.append(".est")
				.toString();
	}
	
	public static void numS(Selection selection, int slices, int probes, int samples, SampledHistogram attHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = Numerical_stochastic.estimateStatic(selection, slices, attHist.sampleSize, samples, probes, attHist);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, numSEstName(namePrefix, slices, probes, samples)));
	}
	
	public static String numSEstName(String namePrefix, int slices, int samples, int probes) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".numS.")
				.append(slices)
				.append(".")
				.append(probes)
				.append(".")
				.append(samples)
				.append(".est")
				.toString();
	}
	
	public static void numPS(Selection selection, int slices, int probes, int samples, SampledHistogram attHist, RankHistogram hist, String path, String namePrefix) throws IOException {
		var est = Numerical_stochasticAndDomainPruning.estimateStatic(selection, slices, attHist.sampleSize, samples, probes, attHist);
		est = ReintroduceRanks.recalculate(est, hist);
		est.writeFile(Path.of(path, numPSEstName(namePrefix, slices, probes, samples)));
	}
	
	public static String numPSEstName(String namePrefix, int slices, int samples, int probes) {
		return new StringBuilder()
				.append(namePrefix)
				.append(".numPS.")
				.append(slices)
				.append(".")
				.append(probes)
				.append(".")
				.append(samples)
				.append(".est")
				.toString();
	}
	
	public static String statHeader = "accuracy, inaccuracy, estimation, histogram";
	
	public static String statLine(Path estPath, Path histPath, int dataSize) throws IOException {
		var est = RankHistogram.readFile(estPath.toString());
		var hist = RankHistogram.readFile(histPath.toString());
		
		return statLine(est, 
				estPath.getFileName().toString(), 
				hist, 
				histPath.getFileName().toString(), 
				dataSize);
	}
	
	public static String statLine(
			RankHistogram est,
			String estName, 
			RankHistogram hist,
			String histName,
			int dataSize) {
		var accuracy = Measurement.accuracy(est, hist, dataSize);
		var inaccuracy = Measurement.inaccuracy(est, hist, dataSize);
		
		return new StringBuilder()
				.append(accuracy).append(",")
				.append(inaccuracy).append(",")
				.append(estName).append(",")
				.append(histName)
				.toString();
	}
	
	public static String statLine(
			RankHistogram est,
			String estName, 
			RankHistogram hist,
			String histName,
			String estId,
			int dataSize) {
		var accuracy = Measurement.accuracy(est, hist, dataSize);
		var inaccuracy = Measurement.inaccuracy(est, hist, dataSize);
		
		return new StringBuilder()
				.append(accuracy).append(",")
				.append(inaccuracy).append(",")
				.append(estName).append(",")
				.append(histName).append(",")
				.append(estId)
				.toString();
	}

	public static void main(String[] args) throws CsvValidationException, ClassNotFoundException, IOException, DuplicateAttributeNameException, ColumnOrderingNotInitializedException, ClassNotInContextException, TableRecordSchemaMismatch, DuplicateHeaderWriteException, SchemaNotEqualException, NotSubschemaException, OnOperatornNotApplicableToSchemaException {
		var datasets = List.of(
				VideoGameSales.instance()
				//, AnimeDataset2023.instance()
				//, TopRankedRealMovies.instance()
				//, AmazonBookScrappings.instance()
				//, BeerReviews.instance()
				);
		
		var start = System.currentTimeMillis();
		for(var ds : datasets) {
			System.out.println(ds.getClass().getSimpleName());
			//ds.experiment();
			ds.prepareData();
			ds.preparedDataStatistics();
			ds.projection.query();
		}
		var end = System.currentTimeMillis();
		
		System.out.println("Finished, time: " + Duration.ofMillis(end - start).toString());
	}

}
