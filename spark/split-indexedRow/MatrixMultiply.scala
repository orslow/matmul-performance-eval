import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD

import java.util.NoSuchElementException
//import scala.collection.mutable._

import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream


object MatrixMultiply extends App {

	val conf = new SparkConf().setAppName("SplittedIRMatrixMultiply")
	val sc = new SparkContext(conf)

	//val rank = 9
	//val iteration = 2
	val p = args(0).toInt
	val input1 = args(1).toString
	val input2 = args(2).toString
	val m = args(3).toInt
	val k = args(4).toInt
	val n = args(5).toInt

	val sqlContext = new SQLContext(sc)
	import sqlContext.implicits._

	var tik0 = System.nanoTime()

	// load each matrix
	val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)

	// load each matrix
	val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
	val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

	// to RDD
	val rows1: RDD[Row] = dataset1.rdd
	val rows2: RDD[Row] = dataset2.rdd

	// parse
	val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }

	val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)
	val irMatrix = coordMatrix1.toIndexedRowMatrix
	//val rowMatrix = coordMatrix1.toRowMatrix

	irMatrix.rows.cache
	//rowMatrix.rows.cache

	val grouped= dataset2.map { case Row(m:Int, k:Int, v:Double) => (k, MatrixEntry(m, 0, v) ) }.groupByKey(_._1).mapGroups{ case (k, iter) => (k, iter.map(a => a._2).toList ) }.collect.toList

	val cachedGroup = grouped.map( a => (a._1, new CoordinateMatrix(sc.parallelize(a._2.toSeq), 633432, 1)) )
	cachedGroup.map( a => a._2.entries.cache)

	var tik1 = System.nanoTime()
	cachedGroup.map( a => irMatrix.multiply(a._2.toBlockMatrix.toLocalMatrix).toCoordinateMatrix.entries.filter(b => b.value!=0.0).map( c => c.i+" "+a._1+" "+c.value ) ).map( a => a.saveAsTextFile("/splittedirmResult/"+a.id) )
	var tik2 = System.nanoTime()

	val latency1 = ((tik1-tik0) / 1e9)
	val latency2 = ((tik2-tik1) / 1e9)

	val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
	writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
	writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
	writer.close
}
