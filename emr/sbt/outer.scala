import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

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
    val partitions = args(6).toInt
    val result_dir = args(7).toString
    val lmat_info = input1.split("/").takeRight(2)
    val rmat_info = input2.split("/").takeRight(2)

    val conf = new SparkConf().setAppName("outer_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1)+"/"+partitions)
    val sc = new SparkContext(conf)

    var tik0 = System.nanoTime()

    val rdd1 = sc.textFile(input1, p)
    val rdd2 = sc.textFile(input2, p)

    val parsed_rdd1 = rdd1.map( r => r.split(" "))

    val mKv = parsed_rdd1.flatMap( a => (a(1).toInt, (a(0).toInt, a(2).toDouble)) :: (a(0).toInt, (a(1).toInt, a(2).toDouble)) :: Nil)
    val nKv = rdd2.map(r => r.split(" ")).map(r => (r(0).toInt, (r(1).toInt, r(2).toDouble)))
    val mnJo = mKv.join(nKv, partitions)

    val mult = mnJo.map(x=> (((x._2)._1._1, (x._2)._2._1), (x._2)._1._2 * (x._2)._2._2))

    val result = mult.reduceByKey((x,y) => x+y).map( a => a._1._1 + " " + a._1._2 + " " + a._2)

    var tik1 = System.nanoTime()
    result.saveAsTextFile("/outResult")
    var tik2 = System.nanoTime()

    val latency1 = ((tik1-tik0) / 1e9)
    val latency2 = ((tik2-tik1) / 1e9)
    val latency3 = latency1+latency2

    // size, join partitions, latency1, latency2, total_latency
    val writer = new PrintWriter(new FileOutputStream(new File(result_dir), true))
    writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+",")
    writer.write(m + "-" + k + "-" + n + "," + partitions + ",")
    writer.write(latency1 + "," + latency2 + ",")
    writer.write(latency3 + "\n")
    writer.close
  }
}
