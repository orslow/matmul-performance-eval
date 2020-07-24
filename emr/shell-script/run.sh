#!/bin/bash

sz=ground-truth

mt=com-Amazon
ms=334863


for dm in 32 # 16 8 # driver memory
do
for em in 20 # 10 40 # executor memory
do
for ec in 4 # 2 1 # executor cores
do
for sp in 64 # 32 16 # split size
do
for rp in 64 # 32 16 # random split size
do
for col in 100 # 1000 
do
for density in 00001 # 00005 0001 0005 001 005 01 05 1
do
for rep in 1 2 3 # 4 5
do

# OUTER
printf "${rep}: " >> outer

spark-submit --deploy-mode client --driver-memory ${dm}g --executor-memory ${em}g --executor-cores ${ec} --class MatrixMultiply s3://{S3_ADDRESS}/jars/nocache/out_2.11-1.0.jar \
64 s3://{S3_ADDRESS}/${sz}/${mt}/LEFT_334863_334863 s3://{S3_ADDRESS}/${sz}/${mt}/RIGHT_${ms}_${col}_${density} $ms $ms ${col} $sp outer $ds

hdfs dfs -rm -r /outResult # delete result file for repeat

# HISTORY TO S3
hdfs dfs -get `hdfs dfs -ls /var/log/spark/apps/ | tail -n 1 | awk '{print $NF}'` . # get last line(app done last) of last column(file name)

mv applic* outer_${mt}_N${nn}_${dm}_${em}_`ls . | awk '{print $NF}' | grep applica`

aws s3 cp `ls . | awk '{print $NF}' | grep applica` s3://{S3_ADDRESS}/spark-history/$(date -d '+9 hour' '+%F')/${mt}/outer/ # add to s3 with date info

rm *applica*
# HISTORY TO S3 END



# BREEZE-INNER
printf "${rep}: " >> inner

spark-submit --deploy-mode client --driver-memory ${dm}g --executor-memory ${em}g --executor-cores ${ec} --class MatrixMultiply s3://{S3_ADDRESS}/jars/nocache/breeze-inner_2.11-1.0.jar \
64 s3://{S3_ADDRESS}/${sz}/${mt}/LEFT_334863_334863 s3://{S3_ADDRESS}/${sz}/${mt}/RIGHT_${ms}_${col}_${density} $ms $ms ${col} inner

hdfs dfs -rm -r /breezeInnerResult # delete result file for repeat

# HISTORY TO S3
hdfs dfs -get `hdfs dfs -ls /var/log/spark/apps/ | tail -n 1 | awk '{print $NF}'` . # get last line(app done last) of last column(file name)

mv applic* inner_${mt}_N${nn}_${dm}_${em}_`ls . | awk '{print $NF}' | grep applica`

aws s3 cp `ls . | awk '{print $NF}' | grep applica` s3://{S3_ADDRESS}/spark-history/$(date -d '+9 hour' '+%F')/${mt}/inner/ # add to s3 with date info

rm *applica*
# HISTORY TO S3 END

# INDEXEDROW (NO SPLITS)

printf "${rep}: " >> indexedrow

spark-submit --deploy-mode client --driver-memory ${dm}g --executor-memory ${em}g --executor-cores ${ec} --class MatrixMultiply s3://{S3_ADDRESS}/jars/nocache/indexedrow_2.11-1.0.jar \
64 s3://{S3_ADDRESS}/${sz}/${mt}/LEFT_334863_334863 s3://{S3_ADDRESS}/${sz}/${mt}/RIGHT_${ms}_${col}_${density} $ms $ms ${col} indexedrow

hdfs dfs -rm -r /irResult # delete result file for repeat

# HISTORY TO S3
hdfs dfs -get `hdfs dfs -ls /var/log/spark/apps/ | tail -n 1 | awk '{print $NF}'` . # get last line(app done last) of last column(file name)

mv applic* lump_${mt}_N${nn}_${dm}_${em}_`ls . | awk '{print $NF}' | grep applica`

aws s3 cp `ls . | awk '{print $NF}' | grep applica` s3://{S3_ADDRESS}/spark-history/$(date -d '+9 hour' '+%F')/${mt}/indexedrow/ # add to s3 with date info

rm *applica*
# HISTORY TO S3 END



# BLOCK
# Amazon: 167432 83716 41858 20929

for rpb in 41858
do
printf "${rep}: " >> block

spark-submit --deploy-mode client --driver-memory ${dm}g --executor-memory ${em}g --executor-cores ${ec} --class MatrixMultiply s3://{S3_ADDRESS}/jars/nocache/block-with-fixed-k_2.11-1.0.jar \
64 s3://{S3_ADDRESS}/${sz}/${mt}/LEFT_334863_334863 s3://{S3_ADDRESS}/${sz}/${mt}/RIGHT_${ms}_${col}_${density} $ms $ms ${col} $rpb $ms 50 64 block

hdfs dfs -rm -r /bmResult # delete result file for repeat

# HISTORY TO S3
hdfs dfs -get `hdfs dfs -ls /var/log/spark/apps/ | tail -n 1 | awk '{print $NF}'` . # get last line(app done last) of last column(file name)

mv applic* block_${mt}_N${nn}_${dm}_${em}_`ls . | awk '{print $NF}' | grep applica`

aws s3 cp `ls . | awk '{print $NF}' | grep applica` s3://{S3_ADDRESS}/spark-history/$(date -d '+9 hour' '+%F')/${mt}/block/ # add to s3 with date info

rm *applica*
# HISTORY TO S3 END


done



done
done
done
done
done
done
done
done
