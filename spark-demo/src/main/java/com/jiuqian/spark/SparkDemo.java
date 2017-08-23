package com.jiuqian.spark;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class SparkDemo {
	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile("hdfs://192.168.0.61:9000/user/data/text/words");
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

			private static final long serialVersionUID = 4868209367128242219L;

			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split("\\W"));
			}
		});

		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

			private static final long serialVersionUID = 4936955993986303903L;

			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() { 

			private static final long serialVersionUID = -7084745825065644275L;

			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		
		wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			private static final long serialVersionUID = 3093175715228157890L;

			public void call(Tuple2<String, Integer> pairs) throws Exception {
				if(StringUtils.isBlank(pairs._1 )){
					return;
				}
				System.out.println(pairs._1 + " : " + pairs._2);
			}
		});
		
		// wordsCount.saveAsTextFile("hdfs://192.168.0.61:9000/user/data/text/output/");
		sc.close();
	}
}
