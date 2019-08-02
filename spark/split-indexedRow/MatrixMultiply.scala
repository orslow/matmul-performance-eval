import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql._

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import java.util.NoSuchElementException
//import scala.collection.mutable._

import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream


object MatrixMultiply extends App {

	def denseMaker(A: List[(Int, Int, Double)], k: Int): DenseMatrix = {
	  val values = new Array[Double](k)
	  A.foreach( a => values(a._1) = a._3 )
	  new DenseMatrix(k, 1, values)
	}

	val spark = SparkSession
		.builder()
		.appName("SplittedIRMatrixMultiply")
		.getOrCreate()

	import spark.implicits._

	val p = args(0).toInt
	val input1 = args(1).toString
	val input2 = args(2).toString
	val m = args(3).toInt
	val k = args(4).toInt
	val n = args(5).toInt

	var tik0 = System.nanoTime()

  // load each matrix
  val dataset1 = spark.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = spark.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

  // to RDD
  val rows1: RDD[Row] = dataset1.rdd

  // parse
  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }

	val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)
	val irMatrix = coordMatrix1.toIndexedRowMatrix
	//val rowMatrix = coordMatrix1.toRowMatrix

	irMatrix.rows.cache
	//rowMatrix.rows.cache

	irMatrix.rows.take(1)

	val grouped= dataset2.map { case Row(m:Int, k:Int, v:Double) => (k, (m, 0, v )) }.groupByKey(_._1).mapGroups{ case (k, iter) => (k, iter.map(a => a._2).toList ) }.collect.toList

	grouped.take(1)

	var tik1 = System.nanoTime()
	grouped.map( a => irMatrix.multiply( denseMaker(a._2, k) ).toCoordinateMatrix.entries.filter(b => b.value!=0.0).map( c => c.i+" "+a._1+" "+c.value ) ).map( a => a.saveAsTextFile("/splittedirmResult/"+a.id) )
	var tik2 = System.nanoTime()

	val latency1 = ((tik1-tik0) / 1e9)
	val latency2 = ((tik2-tik1) / 1e9)

	val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
	writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
	writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
	writer.close
}
