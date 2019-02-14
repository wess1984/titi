package titi;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

public class Main {

	public static void main(String[] args) {
		List<Double> numbers = new ArrayList<Double>();
		numbers.add(14.5);
		numbers.add(23.7);
		numbers.add(47.5);
		numbers.add(174.5);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("testSparkRDD1").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Double> numbersRDD = sc.parallelize(numbers);
		numbersRDD = numbersRDD.map(item -> item * 2);
		
		Double resultReduceRDD = numbersRDD.reduce( (value1,value2) -> value1 + value2 );
		System.out.println(" resultReduceRDD = "+resultReduceRDD);
		
	  /* numbersRDD.reduce(new Function2<Double, Double, Double>() {
		
		private static final long serialVersionUID = 7855452894832363369L;

		@Override
		public Double call(Double value1, Double value2) throws Exception {
			// TODO Auto-generated method stub
			return value1+value2;
		}
	});*/
		
		//numbersRDD.foreach(v -> System.out.println(v));
		numbersRDD.foreach(System.out::println);
		
		//System.out.println("RDD size = "+numbersRDD.count());

		sc.close();


	}

}
