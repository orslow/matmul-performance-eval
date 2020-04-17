#!/bin/bash

for sz in mid # matrix type ( mid / mid / large / xlarge )
do
for mt in 8-0 # specific matrix type
do
for ms in 334863 # matrix size
do
for dm in 50 # driver memory
do
for em in 20 # executor memory
do
for ec in 4 # executor cores
do
for sp in 64 # split size
do
for rp in 64 # random split size
do
for col in 100 # 100 1000 10000 # 1000 10000
do
for density in 00001 00005 0001 0005 001 005 01 05 1 # 5
do
for rep in 1 # 2 # 3 4 5
do

# RAND MATRIX GENERATE
spark-submit --deploy-mode client --driver-memory ${dm}g --executor-memory ${em}g --executor-cores ${ec} --class MatrixGenerator s3://{JAR_FILE_S3_ADDRESS}/randomgenerator_2.11-1.0.jar \
${ms} ${col} 0.${density} ${rp} /R${density}

hdfs dfs -cat /R${density}/* > RIGHT_${ms}_${col}_${density}

hdfs dfs -rm -r /R${density}

done
done
done
done
done
done
done
done
done
done
done

mkdir tmp
mv RIGHT* tmp
aws s3 cp tmp/ s3://{OUTPUT_FILE_WILL_BE_SAVED} --recursive
rm -rf tmp
