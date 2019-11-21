package me.steffenjacobs.apache_spark_example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Steffen Jacobs
 */
public class CharCountExample {

	public static void main(String[] args) {
		new CharCountExample().charCount("./test.txt");
	}

	private void charCount(String filename) {
		// Create a configuration to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Charcounter");

		// Create a Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the Wikipedia article
		JavaRDD<String> input = sc.textFile(filename);

		// split the input string into chars
		JavaRDD<String> chars = input.flatMap(s -> Arrays.asList(s.split("")).iterator());

		// transform the collection of chars into pairs
		// (char, 1) and then count them
		JavaPairRDD<String, Integer> counts = chars.mapToPair(t -> new Tuple2<>(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

		counts = counts.mapToPair(item -> item.swap()).sortByKey(false, 1).mapToPair(item -> item.swap());

		// Take the top 10 elements from RDD
		List<Tuple2<String, Integer>> list = counts.take(10);

		// Output the top 10 elements
		for (Tuple2<String, Integer> t : list) {
			System.out.printf("'%s' - %d\n", t._1(), t._2());
		}

		// print counts to console
		System.out.println(counts);
		// Save the char counts back out to a file.
		// counts.saveAsTextFile("output");

		// close the spark context
		sc.close();
	}
}
