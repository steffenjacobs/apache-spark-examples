package me.steffenjacobs.apache_spark_example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/** @author Steffen Jacobs */
public class WordCountExample {

	public static void main(String[] args) {
		new WordCountExample().countWords("./test.txt");
	}

	private void countWords(String filename) {
		// Create a configuration to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCounter");

		// Create a Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the Wikipedia article line by line
		JavaRDD<String> inputLines = sc.textFile(filename);

		// trim and split the lines into words (1st transformation)
		JavaRDD<String> words = inputLines.flatMap(s -> {
			return Arrays.asList(s.trim().split(" ")).iterator();
		});

		// remove punctuation (2nd transformation)
		JavaRDD<String> punctuationRemovedWords = words.map(w -> {
			return w.replaceAll(",|\\.|\\(|\\)|;|:", "");
		});

		// remove stop words (3rd transformation)
		JavaRDD<String> stopWordRemovedWords = punctuationRemovedWords.filter(s -> {
			return !s.equalsIgnoreCase("the");
		});

		// count the entries in the RDD containing the words (aggregations on
		// intermediate steps)
		long cnt = words.count();
		long cnt2 = punctuationRemovedWords.count();
		long cnt3 = stopWordRemovedWords.count();
		System.out.println("Total words : " + cnt);
		System.out.println("Words without puncutation: " + cnt2);
		System.out.println("Total words without stopwords: " + cnt3);

		// aggregate count by word (6th transformation + aggregation)
		JavaPairRDD<String, Long> wordCounts = stopWordRemovedWords.mapToPair(s -> new Tuple2<>(s, 1l)).reduceByKey((s1, s2) -> s1 + s2);

		// sort by value and therefore swap all tuples (5th-7th transformation)
		JavaPairRDD<String, Long> wordCountSorted = wordCounts.mapToPair(i -> i.swap()).sortByKey(false, 1).mapToPair(i2 -> i2.swap());

		// take the top 10
		List<Tuple2<String, Long>> top10 = wordCountSorted.take(10);

		// print them
		top10.forEach(w -> System.out.printf("%s: %d\n", w._1(), w._2()));

		// close the spark context
		sc.close();

	}
}
