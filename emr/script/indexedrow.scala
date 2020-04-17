import org.apache.spark.sql.SQLContext

import org.apache.spark.mllib.linalg.distributed.{ MatrixEntry, CoordinateMatrix, BlockMatrix }

val p = 64
val input1 = "s3://jueon-matrix-data/snap/ground-truth/email-Eu-core/LEFT_1005_1005"
val input2 = "s3://jueon-matrix-data/snap/ground-truth/email-Eu-core/RIGHT_1005_1000_001"
//val input1 = "/zeppelin/data/email-Eu-core-left.txt"
//val input2 = "/zeppelin/data/email-Eu-core-right_1000.txt"
val m = 1005
val k = 1005
val n = 1000
val result_dir = "/zeppelin/results/outer"
val lmat_info = input1.split("/").takeRight(2)
val rmat_info = input2.split("/").takeRight(2)

/*
val conf = new SparkConf().setAppName("full_ir_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1))
val sc = new SparkContext(conf)
*/

val sqlContext = new SQLContext(sc)

var tik0 = System.nanoTime()

// load each matrix
val rdd1 = sc.textFile(input1, p)
val rdd2 = sc.textFile(input2, p)

val me1 = rdd1.map( row => row.split(" ")).map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble))
val me2 = rdd2.map( row => row.split(" ")).map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble))

val coordMatrix1 = new CoordinateMatrix(me1, m, k)
val coordMatrix2 = new CoordinateMatrix(me2, k, n)

val leftMat = coordMatrix1.toIndexedRowMatrix
val rightMat = coordMatrix2.toBlockMatrix.toLocalMatrix

var tik1 = System.nanoTime()
leftMat.multiply(rightMat).toCoordinateMatrix.entries.filter( b => b.value != 0.0).map(a => a.i+" "+a.j+" "+a.value).saveAsTextFile("/irResult")
var tik2 = System.nanoTime()

val latency1 = ((tik1-tik0) / 1e9)
val latency2 = ((tik2-tik1) / 1e9)
val latency3 = latency1 + latency2

/*
val writer = new PrintWriter(new FileOutputStream(new File(result_dir),true))
writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+",")
writer.write(m + "-" + k + "-" + n + ",")
writer.write(latency1 + "," + latency2 + ",")
writer.write(latency3 + "\n")
*/
