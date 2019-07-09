#!/bin/bash

#./run.sh <num_executors> <num_partitions> <input_hdfs_directory_1> <input_hdfs_directory_2> <m> <k> <n>

#spark-submit --num-executors <num_executors> --class MatrixMultiply --master yarn with_args.jar <num_partitions> <left_input_matrix_directory> <right_input_matrix_directory> <output_directory>

printf "$(date) $line\n"
begin=`date +%s`
#spark-submit --master spark://kef-master:7077 --num-executors $1 --class MatrixMultiply outer-matrix-multiply.jar $2 $3 $4 $5 $6 $7
spark-submit --master yarn --num-executors $1 --class MatrixMultiply outer-matrix-multiply.jar $2 $3 $4 $5 $6 $7
fin=`date +%s`
printf "$(date) $line\n"
runtime=$((fin-begin))
printf "Execution time: $runtime\n\n"
#echo "Execution time #$i: $runtime" >> result

#hdfs dfs -rm -r /outerProductResult

#hdfs dfs -rm -r /results/executors_$1_partitions_$2_result

# stop-all and start-all for clean up memory?
# sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"?

#./run.sh 60 60 /bfs-spark/100K/M /bfs-spark/100K/N 100000 100000 100

#./run.sh 60 60 /bfs-spark/million/M /bfs-spark/million/N 1000000 1000000 100
