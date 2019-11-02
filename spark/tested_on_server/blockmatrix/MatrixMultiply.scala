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

  // ./run.sh 12 60 /ldbc/s/7-5/M /ldbc/s/7-5/N 633432 633432 100 1000 1000 100 500
  val p = args(0).toInt
  val input1 = args(1).toString
  val input2 = args(2).toString
  val m = args(3).toInt
  val k = args(4).toInt
  val n = args(5).toInt

  // split then split
  val mPerBlock = args(6).toInt
  val kPerBlock = args(7).toInt
  val nPerBlock = args(8).toInt
  val midSplits = args(9).toInt

  val sqlContext = new SQLContext(sc)

  var tik0 = System.nanoTime()

  // load each matrix
  val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

  // to RDD
  val rows1: RDD[Row] = dataset1.rdd
  val rows2: RDD[Row] = dataset2.rdd

  // parse
  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }
  val matrixEntries2: RDD[MatrixEntry] = rows2.map { case Row(k:Int, n:Int, v:Double) => MatrixEntry(k, n, v) }

  // MatrixEntry to CoordinateMatrix
  val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)
  val coordMatrix2 = new CoordinateMatrix(matrixEntries2, k, n)

  val blockMatrix1 = coordMatrix1.toBlockMatrix(mPerBlock, kPerBlock)
  val blockMatrix2 = coordMatrix2.toBlockMatrix(kPerBlock, nPerBlock)

  blockMatrix1.cache
  blockMatrix2.cache

  var tik1 = System.nanoTime()
  blockMatrix1.multiply(blockMatrix2, midSplits).toCoordinateMatrix.entries.filter(a => a.value != 0.0).map(a => a.i+" "+a.j+" "+a.value).saveAsTextFile("/blockMultiplyResult")
  var tik2 = System.nanoTime()


  val latency1 = ((tik1-tik0) / 1e9)
  val latency2 = ((tik2-tik1) / 1e9)

  val writer = new PrintWriter(new FileOutputStream(new File("results"), true))
  writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
  writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
  writer.close
}
