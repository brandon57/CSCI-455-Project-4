package main;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class Spark {
	
	public static Table chromTable = new Table();
	private static Pattern space = Pattern.compile("\\s+");
	
	public static void main(String[] args) {
		if(args.length < 1)
		{
			System.err.println("Usage: Spark <file>");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession.builder().appName("Data Counting").getOrCreate();
		
		JavaRDD<String> line = spark.read().textFile(args[0]).javaRDD();
		
		JavaRDD<String> interactions = line.map(s -> Arrays.asList(space.split(s)).iterator());
		
		String
		
		

	}

}
