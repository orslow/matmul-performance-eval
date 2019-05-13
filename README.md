# matmul-performance-eval

## Hadoop

코드 내에서 setNumReduceTasks 설정. 갯수에 따라 결과 바뀜.

run.sh: 코드 jar파일로 build하고 실행.

### HDFS

코드 내에서 matrix size 직접 설정해야 함. -> 이렇게 진행해도 문제 없는지 궁금(현재 MR작업에 들어가는 matrix size는 100 * 500000 * 100

M, N 파일 형식을 각각 i, j, v / j, k, v가 아닌 M, i, j, v / N, j, k, v로 넣어야 함(map 작업에 두개의 파일을 구분해서 넣을 수 없기 때문에 map 코드 내에서 구분하여 처리) 

M, N 파일 크기가 작기 때문에 hdfs dfs -put 할 때 -Ddfs.block.size default보다 작게 넣어야 함. (default 일 떄 4개 블럭밖에 생성되지 않아 yarn node -list 했을 때 container 갯수 5개밖에 생성되지 않음)

M과 N 각각 N의 열, M의 행 갯수만큼 수를 만들기 때문에 matrix size가 커질수록 급격히 성능 하락(정확히는 M의 행 크기, N의 열 크기가 커질수록. 코드 참고) 


./run.sh <input_hdfs_directory> <output_hdfs_directory>

ex) ./run.sh /big/input /big/output

### Mongo

sharding 해놓은 collection에 query를 넣어 temp 만들기, sharding 해놓지 않은 collection에 query를 넣어 temp 만들기

temp collection을 만드는 속도에서 차이를 보일 줄로 알았는데 shard나 noshard나 temp collection을 만드는 속도가 같고 오히려 MR작업에서 속도차이가 남 -> 이유 찾아보기

$out으로 만들어지는 collection은 sharding이 되지 않아 sharding을 통해 얻을 수 있을 성능적 이득은 얻기 어려워 보임.


./run.sh <mongos_container_ip_address> <input_database> <input_collection_name> <output_database> <output_collection_name>

ex) ./run.sh 10.0.139.199 shard input result shard_result

MR작업 여러번 수행 할 때 collection 이름이 겹치지 않도록 전에 만들어놓은 temp collection 삭제 해야 함. 


## Spark

### HDFS

#### CoordinateMatrix -> BlockMatrix

./run.sh <num_executors> <num_partitions> <right_matrix_hdfs_directory> <left_matrix_hdfs_directory>

ex) ./run.sh 60 90 /spark/data/M /spark/data/N


#### Outer product (CoordinateMatrix)

[참고](https://medium.com/balabit-unsupervised/scalable-sparse-matrix-multiplication-in-apache-spark-c79e9ffc0703)


./run.sh <num_executors> <num_partitions> <right_matrix_hdfs_directory> <left_matrix_hdfs_directory>

ex) ./run.sh 60 90 /spark/data/M /spark/data/N


### Mongo

- 
