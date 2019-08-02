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

  val spark = SparkSession
    .builder()
    .appName("LumpIRMatrixMultiply")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext

  val p = args(0).toInt
  val input1 = args(1).toString
  val input2 = args(2).toString
  val m = args(3).toInt
  val k = args(4).toInt
  val n = args(5).toInt

  val h = 5 // split size(to argument?)

  var tik0 = System.nanoTime()

  // load each matrix
  val dataset1 = spark.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = spark.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)

  // to RDD
  val rows1: RDD[Row] = dataset1.rdd
  val rows2: RDD[Row] = dataset2.rdd

  // parse
  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }

  val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)

  val irMatrix = coordMatrix1.toIndexedRowMatrix

  irMatrix.rows.cache
  irMatrix.rows.take(1)

  val r1 = rows2.map( { case Row(k:Int, n:Int, v:Double) => (k, n, v) } )

  val so1 = r1.groupBy(a => a._2).collect

  val jari = Array.fill[Array[Int]](h)(Array.empty[Int])
  val sk = Array.fill[Array[MatrixEntry]](h)(Array.empty[MatrixEntry])

  // save block positions on "jari" and values on "sk"
  so1.map( a => ( jari( a._1 % h ) = jari( a._1 % h ) :+ a._1, a._2.foreach( b => sk( a._1 % h ) = sk( a._1 % h ):+ MatrixEntry(b._1, jari( a._1 % h ).size-1, b._3 ) ) ) )

  val sk2 = sk.zipWithIndex

  var tik1 = System.nanoTime()
  sk2.map( a => irMatrix.multiply(new CoordinateMatrix(sc.parallelize(a._1.toSeq), k, jari(a._2).size).toBlockMatrix.toLocalMatrix ).toCoordinateMatrix.entries.filter( b => b.value != 0.0).map(c => c.i+ " " + jari(a._2)(c.j.toInt) + " " + c.value ) ).map( d => d.saveAsTextFile("/LumpResult/"+d.id) )
  var tik2 = System.nanoTime()

  val latency1 = ((tik1-tik0) / 1e9)
  val latency2 = ((tik2-tik1) / 1e9)

  val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
  writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
  writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
  writer.close
}
