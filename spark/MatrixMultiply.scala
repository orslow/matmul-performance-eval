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

  val conf = new SparkConf().setAppName("MovieLensNMFBlockMatrix")
  val sc = new SparkContext(conf)

  val p = args(0).toInt // partitions on load input file
  val input1 = args(1).toString 
  val input2 = args(2).toString
  val outDir = args(3).toString
  //val numMidSplits

  val sqlContext = new SQLContext(sc)

  var tik = System.nanoTime()

  // load each matrix
  val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

  // to RDD
  val rows1: RDD[Row] = dataset1.rdd
  val rows2: RDD[Row] = dataset2.rdd

  // to MatrixEntry
  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(userId:Int, movieId:Int, rating:Int) => MatrixEntry(userId, movieId, rating) }
  val matrixEntries2: RDD[MatrixEntry] = rows2.map { case Row(userId:Int, movieId:Int, rating:Int) => MatrixEntry(userId, movieId, rating) }

  // MatrixEntry to CoordinateMatrix
  val coordMatrix1 = new CoordinateMatrix(matrixEntries1)
  val coordMatrix2 = new CoordinateMatrix(matrixEntries2)

  // Coordinate Matrix to BlockMatrix (to compute)
  val matA: BlockMatrix = coordMatrix1.toBlockMatrix()
  val matB: BlockMatrix = coordMatrix2.toBlockMatrix()

  // result
  val resBlockMatrix = matA.multiply(matB)
  //val resBlockMatrix = matA.multiply(matB, numMidDimSplits)

  // parse to save on hdfs
  val locMatrix = resBlockMatrix.toLocalMatrix

  val lm: List[Array[Double]] = locMatrix.transpose.toArray.grouped(locMatrix.numCols).toList

  val lines: List[String] = lm.map(line => line.mkString(" "))

  //sc.parallelize(lines).repartition(1).saveAsTextFile("hdfs:///big/output")

  sc.parallelize(lines).saveAsTextFile("hdfs:///results/"+outDir)

  val latency = (System.nanoTime() - tik) / 1e9

  println("[*] Total execution time  : " + latency + " sec")

  // append execution time to file
  //new PrintWriter(outDir) { write("[*] Total execution time  : " + latency + " sec"); close }
  val writer = new PrintWriter(new FileOutputStream(new File(outDir),true))
  writer.write("[*] Total execution time  : " + latency + " sec\n")
  writer.close
}
