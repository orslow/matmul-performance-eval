#!/bin/bash

#./run.sh <num_executors> <num_partitions> <input_hdfs_directory_1> <input_hdfs_directory_2> <left_matrix_row_size> <left_mat_col/right_mat_row_size> <right_matrix_col_size> <mPerBlock> <kPerBlock> <nPerBlock>

#spark-submit --num-executors <num_executors> --class MatrixMultiply --master yarn with_args.jar <num_partitions> <left_input_matrix_directory> <right_input_matrix_directory> <left_matrix_row_size> <left_mat_col/right_mat_row_size> <right_matrix_col_size> <mPerBlock> <kPerBlock> <nPerBlock> <numMidSplits>


# Example) 60 executors / 200K-200K-100 size input matrix / 60 splits on each inputed matrix / perBlock 10000-10000-900
#./run.sh 60 60 /bfs-spark/200K/M /bfs-spark/200K/N 200000 200000 100 10000 10000 900

printf "$(date) $line\n"
begin=`date +%s`
spark-submit --master yarn --num-executors $1 --class MatrixMultiply block-matrix-multiply.jar $2 $3 $4 $5 $6 $7 $8 $9 $10 $11
fin=`date +%s`
printf "$(date) $line\n"
runtime=$((fin-begin))
printf "Execution time: $runtime\n\n"
#echo "Execution time #$i: $runtime"

hdfs dfs -rm -r /blockMatrixResult

# stop-all and start-all for clean up memory?
# sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"?
