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

	val conf = new SparkConf().setAppName("BlockMatrixMultiply")
	val sc = new SparkContext(conf)

	//val rank = 9
	//val iteration = 2
	val p = args(0).toInt
	val input1 = args(1).toString
	val input2 = args(2).toString
	val m = args(3).toInt
	val k = args(4).toInt
	val n = args(5).toInt

	// rows/cols per block
	val mPerBlock = args(6).toInt
	val kPerBlock = args(7).toInt
	val nPerBlock = args(8).toInt

	val sqlContext = new SQLContext(sc)

	var tik0 = System.nanoTime()

	// load each matrix
	val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
	val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

	// to RDD
	val rows1: RDD[Row] = dataset1.rdd
	val rows2: RDD[Row] = dataset2.rdd

	// parse
	val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }
	val matrixEntries2: RDD[MatrixEntry] = rows2.map { case Row(k:Int, n:Int, v:Int) => MatrixEntry(k, n, v.toDouble) }

	// MatrixEntry to CoordinateMatrix
	val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)
	val coordMatrix2 = new CoordinateMatrix(matrixEntries2, k, n)

	// Coordinate Matrix to BlockMatrix (to compute)
	val matA: BlockMatrix = coordMatrix1.toBlockMatrix(mPerBlock, kPerBlock)
	val matB: BlockMatrix = coordMatrix2.toBlockMatrix(kPerBlock, nPerBlock)

	matA.cache
	matB.cache

	matA.blocks.take(1)
	matB.blocks.take(1)

	//matA.validate
	//matB.validate

	val resBlockMatrix = matA.multiply(matB)

	val tik1 = System.nanoTime()
	resBlockMatrix.toCoordinateMatrix.entries.saveAsTextFile("/blockMatrixResult")
	//resBlockMatrix.blocks.take(1)
	//resBlockMatrix.validate
	val tik2 = System.nanoTime()

	val latency1 = ((tik1-tik0) / 1e9)
	val latency2 = ((tik2-tik1) / 1e9)

	val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
	writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + " / " + "rows/colsPerBlock: " + mPerBlock + "-" + kPerBlock + "-" + nPerBlock+ "\n")
	writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
	writer.close
}
