#!/bin/bash

#./run.sh <num_executors> <num_partitions> <input_hdfs_directory_1> <input_hdfs_directory_2>

#spark-submit --num-executors <num_executors> --class MatrixMultiply --master yarn with_args.jar <num_partitions> <left_input_matrix_directory> <right_input_matrix_directory> <output_directory>

spark-submit --master yarn --num-executors $1 --class MatrixMultiply no_mid.jar $2 $3 $4 executors_$1_partitions_$2_result

# remove result file for for loop 
hdfs dfs -rm -r /results/executors_$1_partitions_$2_result

# stop-all and start-all for clean up memory?
# sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
