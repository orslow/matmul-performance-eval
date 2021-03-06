import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import breeze.linalg.SparseVector

import java.io.PrintWriter
import java.io.File
import java.io.FileOutputStream

object MatrixMultiply {

  def main(args: Array[String]) {

    val p = args(0).toInt
    val input1 = args(1).toString
    val input2 = args(2).toString
    val m = args(3).toInt
    val k = args(4).toInt
    val n = args(5).toInt
    val result_dir = args(6).toString
    val lmat_info = input1.split("/").takeRight(2)
    val rmat_info = input2.split("/").takeRight(2)

    val conf = new SparkConf().setAppName("breeze-inner_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1))
    val sc = new SparkContext(conf)

    var tik0 = System.nanoTime()

    val rdd1 = sc.textFile(input1, p)
    val rdd2 = sc.textFile(input2, p)

    val parsed_rdd1 = rdd1.map(r => r.split(" "))

    val me1 = rdd1.map( r => r.split(" ")).flatMap( a => (a(0).toInt, (a(1).toInt, a(2).toDouble)) :: (a(1).toInt, (a(0).toInt, a(2).toDouble)) :: Nil)
    val lRowGrouped = me1.groupByKey()
    val lSparse = lRowGrouped.map(x => (x._1, x._2.toSeq.sortBy(_._1).unzip))
    val lBreezeSparse = lSparse.map(x => (x._1, new SparseVector(x._2._1.toArray, x._2._2.toArray, k)))

    val me2 = rdd2.map( a => a.split(" ")).map( a => (a(1).toInt, (a(0).toInt, a(2).toDouble)))
    val rightMat = me2.groupByKey()
    val rSparse = rightMat.map(x => (x._1, x._2.toSeq.sortBy(_._1).unzip))
    val rBreezeSparse = rSparse.map(x => (x._1, new SparseVector(x._2._1.toArray, x._2._2.toArray, k)))

    val bRight = sc.broadcast(rBreezeSparse.collect)

    val result = lBreezeSparse.flatMap{ case(lIndex, lVector) => {bRight.value.map(x => ((lIndex, x._1), lVector.dot(x._2)))}}.filter(x => x._2 != 0.0).map( r => r._1._1 + " " + r._1._2 + " " + r._2)

    var tik1 = System.nanoTime()
    result.saveAsTextFile("/breezeInnerResult")
    var tik2 = System.nanoTime()

    val latency1 = ((tik1-tik0) / 1e9)
    val latency2 = ((tik2-tik1) / 1e9)
    val latency3 = latency1+latency2

    val writer = new PrintWriter(new FileOutputStream(new File(result_dir), true))
    writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+"/"+p+",")
    writer.write(m + "-" + k + "-" + n + ",")
    writer.write(latency1 + "," + latency2 + ",")
    writer.write(latency3 + "\n")
    writer.close
  }
}
