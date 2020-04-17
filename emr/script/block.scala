//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf

import org.apache.spark.mllib.linalg.distributed.{ CoordinateMatrix, MatrixEntry, BlockMatrix }

val p = 64
val input1 = "s3://jueon-matrix-data/snap/ground-truth/email-Eu-core/LEFT_1005_1005"
val input2 = "s3://jueon-matrix-data/snap/ground-truth/email-Eu-core/RIGHT_1005_1000_001"
//val input1 = "/zeppelin/data/email-Eu-core-left.txt"
//val input2 = "/zeppelin/data/email-Eu-core-right_1000.txt"
val m = 1005
val k = 1005
val n = 1000
val partitions = 64

// block split options
val mPerBlock = 252
val kPerBlock = m
val nPerBlock = 50 // manually set
val midSplits = 64
//val result_dir = "/zeppelin/results/block"
val lmat_info = input1.split("/").takeRight(2)
val rmat_info = input2.split("/").takeRight(2)

/*
val conf = new SparkConf().setAppName("block_"+m+"-"+k+"-"+n+"-"+rmat_info(0)+"-"+rmat_info(1)+"/"+mPerBlock+"-"+k+"-"+nPerBlock+"&"+midSplits)
val sc = new SparkContext(conf)
*/

val tik0 = System.nanoTime()

val rdd1 = sc.textFile(input1, p)
val rdd2 = sc.textFile(input2, p)

val parsed_rdd1 = rdd1.map( a => a.split(" "))

val me1 = parsed_rdd1.map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble))
val me2 = rdd2.map( a => a.split(" ")).map( a => MatrixEntry(a(0).toInt, a(1).toInt, a(2).toDouble))

val leftMat = new CoordinateMatrix(me1, m, k).toBlockMatrix(mPerBlock, k)
val rightMat = new CoordinateMatrix(me2, k, n).toBlockMatrix(k, nPerBlock)

//leftMat.blocks.count
//rightMat.blocks.count
//leftMat.validate
//rightMat.validate

val tik1 = System.nanoTime()
leftMat.multiply(rightMat, midSplits).toCoordinateMatrix.entries.filter(a => a.value != 0.0).map(a => a.i+" "+a.j+" "+a.value).saveAsTextFile("/bmResult")
val tik2 = System.nanoTime()

val latency1 = ((tik1-tik0) / 1e9)
val latency2 = ((tik2-tik1) / 1e9)
val latency3 = latency1 + latency2

/*
// size, perBlockInfo, latency1, latency2, total_latency
val writer = new PrintWriter(new FileOutputStream(new File(result_dir), true))
writer.write(lmat_info(0)+"-"+lmat_info(1)+","+rmat_info(0)+"-"+rmat_info(1)+",")
writer.write(m + "-" + k + "-" + n + ",")
writer.write(mPerBlock + "-" + k + "-" + nPerBlock + "/" + midSplits + ",")
writer.write(latency1 + "," + latency2 + "," + latency3 + "\n")
writer.close
*/
