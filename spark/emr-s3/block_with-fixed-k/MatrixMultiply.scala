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
  val result_dir = args(10).toString
  val lmat_info = input1.split("/").takeRight(2)
  val rmat_info = input2.split("/").takeRight(2)

  val conf = new SparkConf().setAppName("block_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1))
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val tik0 = System.nanoTime()

  val rdd1 = sc.textFile(input1)
  val rdd2 = sc.textFile(input2)
  
  val parsed_rdd1 = rdd1.map( a => a.split(" "))

  val me1 = parsed_rdd1.map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble)) ++ parsed_rdd1.map( a => MatrixEntry(a(1).toInt, a(0).toInt, a(2).toDouble))
  val me2 = rdd2.map( a => a.split(" ")).map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble))

  val leftMat = new CoordinateMatrix(me1, m, k).toBlockMatrix(mPerBlock, k).cache
  val rightMat = new CoordinateMatrix(me2, k, n).toBlockMatrix(k, nPerBlock).cache

  leftMat.validate
  rightMat.validate

  val result = leftMat.multiply(rightMat, midSplits).toCoordinateMatrix.entries.filter(a => a.value != 0.0).map(a => a.i+" "+a.j+" "+a.value)

  val tik1 = System.nanoTime()
  result.saveAsTextFile("/bmResult")
  //result.validate
  val tik2 = System.nanoTime()

  val latency1 = ((tik1-tik0) / 1e9)
  val latency2 = ((tik2-tik1) / 1e9)
  val latency3 = latency1 + latency2

  // size, perBlockInfo, latency1, latency2, total_latency
  val writer = new PrintWriter(new FileOutputStream(new File(result_dir), true))
  writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+",")
  writer.write(m + "-" + k + "-" + n + ",")
  writer.write(mPerBlock + "-" + k + "-" + nPerBlock + "/" + midSplits + ",")
  writer.write(latency1 + "," + latency2 + "," + latency3 + "\n")
  writer.close

}

