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


  val p = args(0).toInt // numPartitions on load file
  val input1 = args(1).toString
  val input2 = args(2).toString
  val m = args(3).toInt // M row size
  val k = args(4).toInt // M col / N row size
  val n = args(5).toInt // N col size
  val h = args(6).toInt // split size
  val result_dir = args(7).toString
  val lmat_info = input1.split("/").takeRight(2)
  val rmat_info = input2.split("/").takeRight(2)

  val conf = new SparkConf().setAppName("inner_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  var tik0 = System.nanoTime()

  // load each matrix
  val rdd1 = sc.textFile(input1)
  val rdd2 = sc.textFile(input2)

  val parsed_rdd1 = rdd1.map( r => r.split(" "))

  val me1 = parsed_rdd1.map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble)) ++ parsed_rdd1.map( a => MatrixEntry(a(1).toInt, a(0).toInt, a(2).toDouble))
  val me2 = rdd2.map( a => a.split(" ")).map( a => (a(0).toInt, a(1).toInt, a(2).toDouble))

  val coordMatrix = new CoordinateMatrix(me1, m, k)

  val irMatrix = coordMatrix.toIndexedRowMatrix

  // validate
  irMatrix.rows.cache
  irMatrix.rows.take(1)

  // group by column
  val grouped = me2.groupBy(a => a._2)
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

  val writer = new PrintWriter(new FileOutputStream(new File(result_dir),true))
  writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+",")
  writer.write(m + "-" + k + "-" + n + "," + h + ",")
  writer.write(latency1 + "," + latency2 + ",")
  writer.write(latency3 + "\n")

  writer.close
}

