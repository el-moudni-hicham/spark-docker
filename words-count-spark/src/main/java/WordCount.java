import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Word Count").setMaster("spark://spark-master:7077");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rddLines=sc.textFile("/bitnami/words.txt");

        JavaRDD<String> rddWords=rddLines.flatMap(line-> Arrays.asList(line.split(" ")).iterator());
        JavaPairRDD<String,Integer> pairRDDWords=rddWords.mapToPair(word->new Tuple2<>(word,1));
        JavaPairRDD<String,Integer> wordCount=pairRDDWords.reduceByKey((a,b)->{
            return a+b;});
        wordCount.foreach(e-> System.out.println(e._1+" "+e._2));
    }
}