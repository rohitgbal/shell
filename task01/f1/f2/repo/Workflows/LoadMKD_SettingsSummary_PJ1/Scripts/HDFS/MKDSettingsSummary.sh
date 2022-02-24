#!/bin/bash


HIVE_DATABASE_NAME=${1}
SPARK_MASTER=${2}
SPARK_DEPLOY_MODE=${3}
SPARK_JARS=${4}
SPARK_NETWORK_TIMEOUT=${5}
SPARK_UI_PORT=${6}
SPARK_DRIVER_MEMORY=${7}
SPARK_NUM_EXECUTORS=${8}
SPARK_EXECUTOR_MEMORY=${9}
OOZIE_QUEUE_NAME=${10}
SPARK_SQL_SHUFFLE_PARTITIONS=${11}
SPARK_SQL_ORC_FILTERPUSHDOWN=${12}
SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD=${13}
SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT=${14}
SPARK_TASK_MAXFAILURES=${15}
SPARK_SHUFFLE_IO_MAXRETRIES=${16}
SPARK_DYNAMICALLOCATION_ENABLED=${17}
SPARK_SQL_ORC_ENABLED=${18}
SPARK_SQL_HIVE_CONVERTMETASTOREORC=${19}
SPARK_SQL_ORC_CHAR_ENABLED=${20}
SPARK_FILES=${21}
KERBEROS_PRINCIPAL=${22}
KERBEROS_KEYTAB_FILENAME=${23}
MKD_SETTINGS_QUERY_DAYS=${24}
HIVE_JARS=${25}
PY_SPARK_WAREHOUSE=${26}
CURRENT_DATE=${27}
OUTPUT=${28}
LLAP_CONECTER_JAR=${29}
LLAP_CONECTER_ZIP=${30}
HIVE_BEELINE_SERVER=${31}


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
             --conf spark.driver.extraJavaOptions=-Duser.timezone="UTC" \
             --conf spark.executor.extraJavaOptions=-Duser.timezone="UTC" \
             --conf spark.sql.hive.convertMetastoreOrc=$SPARK_SQL_HIVE_CONVERTMETASTOREORC \
             --conf spark.sql.orc.char.enabled=$SPARK_SQL_ORC_CHAR_ENABLED \
             --jars ${LLAP_CONECTER_JAR} \
             --py-files ${LLAP_CONECTER_ZIP} \
             --conf spark.sql.extensions=com.hortonworks.spark.sql.rule.Extensions \
             --conf spark.kryo.registrator=com.qubole.spark.hiveacid.util.HiveAcidKyroRegistrator \
             --conf spark.sql.hive.hiveserver2.jdbc.url=${HIVE_BEELINE_SERVER} \
             --conf spark.datasource.hive.warehouse.read.mode=DIRECT_READER_V1 \
             --conf spark.driver.maxResultSize='6g' \
             --conf spark.dynamicAllocation.maxExecutors='100' \
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
             MKDSettingsSummary.py  $HIVE_DATABASE_NAME  $MKD_SETTINGS_QUERY_DAYS  $CURRENT_DATE $OUTPUT



if [ "$?" != "0" ]; then
        echo "*** MKD Settings Summary Staging Failed. STOPPING!!!"
        exit 1
fi

exit 0
