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
import java.util.List;
import java.util.Iterator;

public class InteractionsMap implements PairFlatMapFunction<String, String, Integer> {
	
	@Override
	public Iterator<Tuple2<String, Integer>> call(String input)
	{
		List<Tuple2<String, Integer>> pairs = new ArrayList<>();
		
		String[] line = input.split("\\s+");
		if(line.length == 4)
		{
			Integer chrom1 = Integer.parseInt(line[0]);
			long chrom1BinPairs = Long.parseLong(line[1]);
			Integer chrom2 = Integer.parseInt(line[2]);
			long chrom2BinPairs = Long.parseLong(line[3]);
			
			//Checks for errors
			//If there is an error it returns false and it doesn't write it to a file
			if(Spark.chromTable.validBin(chrom1, chrom1BinPairs) && Spark.chromTable.validBin(chrom2, chrom2BinPairs))
			{
				long chrom1Bin = Spark.chromTable.getBin(chrom1, chrom1BinPairs);
				long chrom2Bin = Spark.chromTable.getBin(chrom2, chrom2BinPairs);
				
				if(chrom1Bin > chrom2Bin)
				{
					long temp = chrom1Bin;
					chrom1Bin = chrom2Bin;
					chrom2Bin = temp;
				}
				String result = "(" + chrom1Bin + ", " + chrom2Bin + ") ";
				pairs.add(new Tuple2<>(result, 1));
			}
		}
		return pairs.iterator();
	}
	
	
}
