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

  val kef1 = rows1.map( {case Row(k:Int, n:Int, v:Double) => (k, (n, v)) } )
  val kef2 = rows2.map( {case Row(k:Int, n:Int, v:Double) => (n, (k, v)) } )

  val gk1 = kef1.groupByKey
  val gk2 = kef2.groupByKey

  val carte = gk1.cartesian(gk2)

  carte.cache
  carte.take(1)

  val resultMatrix = carte.map( { case (a, b) => ( (a._1, b._1), (a._2 ++ b._2).groupBy(_._1).values.filter( a => a.size > 1 ).map( a => a.map { case (_, v2) => v2}).map( a => a.reduce(_*_) ).reduceOption(_+_) ) } )

  // filtering empty collection
  val cooMatrix = resultMatrix.filter( a => a._2 != None).map( a => (a._1, a._2.get) )
  //val cooMatrix = resultMatrix.filter( a => a._2 != None)

  var tik1 = System.nanoTime()
  cooMatrix.map( a => a._1._1+" "+a._1._2+" "+a._2).saveAsTextFile("/innerProductResult")
  var tik2 = System.nanoTime()

  val latency1 = ((tik1-tik0) / 1e9)
  val latency2 = ((tik2-tik1) / 1e9)

  val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
  writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
  writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
  writer.close
}
