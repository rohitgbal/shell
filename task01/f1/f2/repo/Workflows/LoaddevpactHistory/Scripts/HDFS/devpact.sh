#!/bin/bash

hive_database_name=${1}
raw_input_location=${2}
raw_inprocess_location=${3}
source_history_table=${4}
outputPath=${5}

SPARK_MASTER=${6}
SPARK_DEPLOY_MODE=${7}
SPARK_JARS=${8}
SPARK_NETWORK_TIMEOUT=${9}
SPARK_UI_PORT=${10}
SPARK_DRIVER_MEMORY=${11}
SPARK_NUM_EXECUTORS=${12}
SPARK_EXECUTOR_MEMORY=${13}
OOZIE_QUEUE_NAME=${14}
SPARK_SQL_SHUFFLE_PARTITIONS=${15}
SPARK_SQL_ORC_FILTERPUSHDOWN=${16}
SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD=${17}
SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT=${18}
SPARK_TASK_MAXFAILURES=${19}
SPARK_SHUFFLE_IO_MAXRETRIES=${20}
SPARK_DYNAMICALLOCATION_ENABLED=${21}
SPARK_SQL_ORC_ENABLED=${22}
SPARK_SQL_HIVE_CONVERTMETASTOREORC=${23}
SPARK_SQL_ORC_CHAR_ENABLED=${24}
SPARK_FILES=${25}
KERBEROS_PRINCIPAL=${26}
KERBEROS_KEYTAB_FILENAME=${27}
Incremental=${28}
current_user=${29}
json_jar1=${30}
json_jar2=${31}
MKD_SCHEDULE_QUERY_DAYS=${32}
CURRENT_DATE=${33}





#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export export SPARK_MAJOR_VERSION=2

spark-submit --conf spark.yarn.executor.memoryOverhead=$SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD \
             --conf spark.sql.orc.filterPushdown=$SPARK_SQL_ORC_FILTERPUSHDOWN \
             --conf spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS \
             --conf spark.core.connection.ack.wait.timeout=$SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT \
             --conf spark.task.maxFailures=$SPARK_TASK_MAXFAILURES \
             --conf spark.shuffle.io.maxRetries=$SPARK_SHUFFLE_IO_MAXRETRIES \
             --conf spark.dynamicAllocation.enabled=$SPARK_DYNAMICALLOCATION_ENABLED \
             --conf spark.sql.orc.enabled=$SPARK_SQL_ORC_ENABLED \
             --conf spark.sql.hive.convertMetastoreOrc=$SPARK_SQL_HIVE_CONVERTMETASTOREORC \
             --conf spark.sql.orc.char.enabled=$SPARK_SQL_ORC_CHAR_ENABLED \
             --conf spark.driver.extraJavaOptions=-Duser.timezone="UTC" \
             --conf spark.executor.extraJavaOptions=-Duser.timezone="UTC" \
             --conf spark.hadoop.metastore.catalog.default=$SPARK_HADOOP_METASTORE \
			 --conf spark.driver.maxResultSize='6g' \
			 --conf spark.dynamicAllocation.maxExecutors='150' \
             --conf spark.dynamicAllocation.minExecutors=25 \
			 --conf spark.network.timeout=1200 \
             --conf spark.sql.broadcastTimeout=1000 \
			 --conf spark.yarn.max.executor.failures=200 \
             --conf spark.task.maxFailures=20 \
             --master $SPARK_MASTER \
             --deploy-mode $SPARK_DEPLOY_MODE \
             --driver-memory $SPARK_DRIVER_MEMORY \
             --executor-memory $SPARK_EXECUTOR_MEMORY \
             --num-executors $SPARK_NUM_EXECUTORS \
             --queue $OOZIE_QUEUE_NAME \
			 devpact.py "$KERBEROS_KEYTAB_FILENAME" "$KERBEROS_PRINCIPAL" "$hive_database_name" "$outputPath" "$Incremental" "$source_history_table" "$raw_input_location" "$raw_inprocess_location" "$current_user" "$json_jar1" "$json_jar2" "$MKD_SCHEDULE_QUERY_DAYS" "$CURRENT_DATE"




if [ "$?" != "0" ]; then
        echo "*** devpact to Staging Failed. STOPPING!!!"
        exit 1
fi

exit 0

