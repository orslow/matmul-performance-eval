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

  def coordinateMatrixMultiply(leftMatrix: CoordinateMatrix, rightMatrix: CoordinateMatrix): CoordinateMatrix = {
    val M_ = leftMatrix.entries
      .map({ case MatrixEntry(i, j, v) => (j, (i, v)) })
    val N_ = rightMatrix.entries
      .map({ case MatrixEntry(j, k, w) => (j, (k, w)) })
    val productEntries = M_
      .join(N_)
      .map({ case (_, ((i, v), (k, w))) => ((i, k), (v * w)) })
      .reduceByKey(_ + _)
      .map({ case ((i, k), sum) => MatrixEntry(i, k, sum) })
    new CoordinateMatrix(productEntries)
  }

  val conf = new SparkConf().setAppName("OuterProductMatrixMultiply")
  val sc = new SparkContext(conf)

  //val rank = 9
  //val iteration = 2
  val p = args(0).toInt
  val input1 = args(1).toString
  val input2 = args(2).toString
  val outDir = args(3).toString

  val sqlContext = new SQLContext(sc)

  var tik = System.nanoTime()

  // load each matrix
  val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

  // to RDD
  val rows1: RDD[Row] = dataset1.rdd
  val rows2: RDD[Row] = dataset2.rdd

  // parse
  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Int) => MatrixEntry(m, k, v) }
  val matrixEntries2: RDD[MatrixEntry] = rows2.map { case Row(k:Int, n:Int, v:Int) => MatrixEntry(k, n, v) }

  // MatrixEntry to CoordinateMatrix
  val coordMatrix1 = new CoordinateMatrix(matrixEntries1)
  val coordMatrix2 = new CoordinateMatrix(matrixEntries2)

  val resultMatrix = coordinateMatrixMultiply(coordMatrix1, coordMatrix2)

  resultMatrix.entries.saveAsTextFile("hdfs:///results/"+outDir)

  /*
  // parse for save on hdfs
  val locMatrix = resBlockMatrix.toLocalMatrix

  val lm: List[Array[Double]] = locMatrix.transpose.toArray.grouped(locMatrix.numCols).toList

  val lines: List[String] = lm.map(line => line.mkString(" "))

  //sc.parallelize(lines).repartition(1).saveAsTextFile("hdfs:///big/output")

  sc.parallelize(lines).saveAsTextFile("hdfs:///results/"+outDir)

  val latency = (System.nanoTime() - tik) / 1e9

  println("[*] Total execution time  : " + latency + " sec")

  //new PrintWriter(outDir) { write("[*] Total execution time  : " + latency + " sec"); close }
  val writer = new PrintWriter(new FileOutputStream(new File(outDir),true))
  writer.write("[*] Total execution time  : " + latency + " sec\n")
  writer.close
  */
}
