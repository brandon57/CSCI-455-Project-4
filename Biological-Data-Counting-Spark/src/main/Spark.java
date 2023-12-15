package main;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Iterator;

public class Spark {
	
	public static Table chromTable = new Table();
	private static Pattern SPACE = Pattern.compile(" ");
	private static Pattern lineSpace = Pattern.compile("\\s+");
	
	public static void main(String[] args) {
		if(args.length < 1)
		{
			System.err.println("Usage: Spark <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession
			      .builder()
			      .appName("Data Counting")
			      .getOrCreate();
		
		//This takes in the input file
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
		
		//This calls the InteractionsMap class and reads each line from the input file
		JavaPairRDD<String, Integer> interactions = lines.flatMapToPair(new InteractionsMap());
		
		//This combines all the pairs that are the same
		JavaPairRDD<String, Integer> counts = interactions.reduceByKey(new Function2<Integer, Integer, Integer>()
		{
			public Integer call(Integer i1, Integer i2)
			{
				return i1 + i2;
			}
		});
		
		//This makes the output look better
		JavaRDD<String> output = counts.map(tuple -> tuple._1() + " " + tuple._2());
		
		output.saveAsTextFile("./output");
	    spark.stop();
	}
}
