import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

object LetterFileIndexCounter
{
	def main(args: Array[String]): Unit = 
	{
		val conf = new SparkConf().setAppName("LetterFileIndexCounter")

		// create a scala spark context
		val sc = new SparkContext(conf)

		// load the text from each file inside the input directory
		val input = sc.wholeTextFiles("file:///home/user/Spark-Letter-File-Index-Counter/input/*")

		// create key-value pairs for each textfile (key) and its words (value)
		val map_by_file = input.map(file_text => (file_text._1, file_text._2))

		// split the words from each file, map them by (letter, filename) 
		// and reduce the occurrences of each letter for each file
		val reduce_letter_file = map_by_file.flatMap(file_words => file_words._2.split("\\s+")
								.map(word => ((file_words._1, word(0).toUpper), 1)))
								.reduceByKey(_+_)

		// map the previous results as (letter, (filename, sum)), reduce them by max(sum)
		// and print the results on the console
		val output = reduce_letter_file.map(key_sum => (key_sum._1._2, (key_sum._1._1, key_sum._2)))
		.reduceByKey {case ((file_1, sum_1), (file_2, sum_2)) => if (sum_2 >= sum_1) (file_2, sum_2) else (file_1, sum_1)}
		.collect()
		.foreach(println)

		sc.stop()
	}
}

