import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry, BlockMatrix }
import org.apache.spark.rdd.RDD

import java.util.NoSuchElementException
import scala.collection.mutable._

import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream

object MatrixMultiply extends App {

	val conf = new SparkConf().setAppName("InnerProductMatrixMultiply")
	val sc = new SparkContext(conf)

	val p = args(0).toInt
	val input1 = args(1).toString
	val input2 = args(2).toString

	// for matrix size information only
	val m = args(3).toInt
	val k = args(4).toInt
	val n = args(5).toInt

	val sqlContext = new SQLContext(sc)

	var tik0 = System.nanoTime()

	// load each matrix
	val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
	val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

	// to RDD
	val rows1: RDD[Row] = dataset1.rdd
	val rows2: RDD[Row] = dataset2.rdd

	// parse
	val matrixEntries1 = rows1.map { case Row(m:Int, k:Int, v:Double) => (m, (k, v)) }
	val matrixEntries2 = rows2.map { case Row(k:Int, n:Int, v:Int) => (n, (k, v)) } // transpose

	val irm1 = matrixEntries1.groupByKey
	val irm2 = matrixEntries2.groupByKey

	// make every pair
	val carte = irm1.cartesian(irm2)

	carte.cache
	carte.take(1)

	val resultMatrix = carte.map( { case (a, b) => ( (a._1, b._1), (a._2 ++ b._2).groupBy(_._1).values.filter( a => a.size > 1 ).map( a => a.map { case (_, v2) => v2}).map( a => a.reduce(_*_) ).reduceOption(_+_) ) } )

	// filtering empty collection
	val CooMatrix = resultMatrix.filter( a => a._2 != None)

	var tik1 = System.nanoTime()
	CooMatrix.saveAsTextFile("/innerProductResult")
	//CooMatrix.take(1)
	//CooMatrix.count // take a lot of time
	var tik2 = System.nanoTime()

	val latency1 = ((tik1-tik0) / 1e9)
	val latency2 = ((tik2-tik1) / 1e9)

	val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
	writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
	writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
	writer.close
}
