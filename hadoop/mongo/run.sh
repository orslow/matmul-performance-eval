#!/bin/bash

mkdir mat

javac -classpath $(hadoop classpath) -d mat MatrixMultiply.java

jar -cvf MatrixMultiply.jar -C mat/ .

for i in 1 #2 3 4
do
printf "$(date) $line\n"
begin=`date +%s`
hadoop jar MatrixMultiply.jar MatrixMultiply $1 $2 $3 $4 $5 $i
fin=`date +%s`
printf "$(date) $line\n"
runtime=$((fin-begin))
printf "Execution time: $runtime\n\n"
echo "Execution time #$i: $runtime" >> $1
done

rm MatrixMultiply.jar

rm -rf mat
