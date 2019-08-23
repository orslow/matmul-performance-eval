import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._

import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import java.util.NoSuchElementException
import scala.collection.mutable._

import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream

object MatrixMultiply extends App {

  def denseMaker(A: List[(Int, Int, Double)], m: Int, k: Int): DenseMatrix = {
    val values = new Array[Double](m*k)
    A.foreach( a => values(a._1+a._2*m) = a._3 )
    new DenseMatrix(m, k, values)
  }

  val conf = new SparkConf().setAppName("LumpIndexedRowMultiply")
  val sc = new SparkContext(conf)

  val p = args(0).toInt // numPartitions on load file
  val input1 = args(1).toString
  val input2 = args(2).toString
  val m = args(3).toInt // M row size
  val k = args(4).toInt // M col / N row size
  val n = args(5).toInt // N col size
  val h = args(6).toInt // split size

  val sqlContext = new SQLContext(sc)

  var tik0 = System.nanoTime()

  // load each matrix
  val dataset1 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input1)
  val dataset2 = sqlContext.read.format("com.databricks.spark.csv").option("delimiter", " ").option("header", "false").option("numPartitions", p).option("inferSchema", true).load("hdfs://"+input2)


  // to RDD
  val rows1: RDD[Row] = dataset1.rdd
  val rows2: RDD[Row] = dataset2.rdd

  val matrixEntries1: RDD[MatrixEntry] = rows1.map { case Row(m:Int, k:Int, v:Double) => MatrixEntry(m, k, v) }

  val coordMatrix1 = new CoordinateMatrix(matrixEntries1, m, k)

  val irMatrix = coordMatrix1.toIndexedRowMatrix

  irMatrix.rows.cache
  irMatrix.rows.take(1)

  // group by column
  val grouped = rows2.map( { case Row(k:Int, n:Int, v:Double) => (k, n, v) } ).groupBy(a => a._2)
  grouped.cache

  
  val jari = Array.fill[Array[Int]](h)(Array.empty[Int])
  val splits = Array.fill[Array[MatrixEntry]](h)(Array.empty[MatrixEntry])

  // use % to split columns
  for(i <- 0 to h-1) {
    val ho = h // should be defined
    jari(i) = grouped.filter(a => ((a._1) % (ho)) == i ).map( a => a._1 ).collect
    splits(i) = grouped.filter(a => ((a._1) % (ho)) == i ).zipWithIndex.map( a => a._1._2.map( b => MatrixEntry(b._1, a._2, b._3) ) ).flatMap( a => a ).collect
  }
  
  // zip splits with jari to calculate then locate
  val hab = splits.zip(jari)
  hab.take(1)

  var tik1 = System.nanoTime()
  hab.map( a => irMatrix.multiply(new CoordinateMatrix(sc.parallelize(a._1.toSeq), k, a._2.size).toBlockMatrix.toLocalMatrix ).toCoordinateMatrix.entries.filter( b => b.value != 0.0).map(c => c.i+ " " + a._2(c.j.toInt) + " " + c.value ) ).map( d => d.saveAsTextFile("/lumpResult/"+d.id) )
  var tik2 = System.nanoTime()

  val latency1 = ((tik1-tik0) / 1e9)
  val latency2 = ((tik2-tik1) / 1e9)
  val latency3 = latency1 + latency2

  val writer = new PrintWriter(new FileOutputStream(new File("results"),true))
  writer.write("\n" + "Matrix size: " + m + "-" + k + "-" + n + "\n")
  writer.write("[*] Execution time  : " + latency1 + " / " + latency2 + "\n")
  writer.write("[*] Total time  : " + latency3 + "\n")
  writer.close
}
