#!/bin/bash

DATABASE=${1}
CURRENT_USER=${2}
CRC4T_STRGYPARTNO_QUERY_DAYS=${3}
SPARK_MASTER=${4}
SPARK_NETWORK_TIMEOUT=${5}
SPARK_UI_PORT=${6}
SPARK_DEPLOY_MODE=${7}
SPARK_JARS=${8}
SPARK_DRIVER_MEMORY=${9}
SPARK_NUM_EXECUTORS=${10}
SPARK_EXECUTOR_MEMORY=${11}
OOZIE_QUEUE_NAME=${12}
SPARK_SQL_SHUFFLE_PARTITIONS=${13}
SPARK_SQL_ORC_FILTERPUSHDOWN=${14}
#SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD=${15}
SPARK_EXECUTOR_MEMORY_OVERHEAD=${15}
SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT=${16}
SPARK_TASK_MAXFAILURES=${17}
SPARK_SHUFFLE_IO_MAXRETRIES=${18}
SPARK_DYNAMICALLOCATION_ENABLED=${19}
SPARK_SQL_ORC_ENABLED=${20}
SPARK_SQL_HIVE_CONVERTMETASTOREORC=${21}
SPARK_SQL_ORC_CHAR_ENABLED=${22}
SPARK_FILES=${23}
KERBEROS_KEYTAB_FILENAME=${24}
KERBEROS_PRINCIPAL=${25}
CURRENT_DATE=${26}
SPARK_EXECUTOR_CORES=${27}


#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export SPARK_MAJOR_VERSION=2

spark-submit --conf spark.yarn.executor.memoryOverhead=$SPARK_EXECUTOR_MEMORY_OVERHEAD \
    --conf spark.hadoop.metastore.catalog.default=hive \
    --conf spark.sql.orc.filterPushdown=$SPARK_SQL_ORC_FILTERPUSHDOWN \
    --conf spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS \
    --conf spark.driver.maxResultSize=6g \
    --conf spark.sql.orc.enabled=$SPARK_SQL_ORC_ENABLED \
    --conf spark.sql.hive.convertMetastoreOrc=$SPARK_SQL_HIVE_CONVERTMETASTOREORC \
    --conf spark.sql.orc.char.enabled=$SPARK_SQL_ORC_CHAR_ENABLED \
    --conf spark.dynamicAllocation.executorIdleTimeout=1000 \
    --conf spark.network.timeout=1200 \
    --conf spark.sql.broadcastTimeout=1000 \
    --conf spark.dynamicAllocation.maxExecutors=$SPARK_NUM_EXECUTORS \
    --conf spark.dynamicAllocation.minExecutors=25 \
    --master $SPARK_MASTER \
    --deploy-mode $SPARK_DEPLOY_MODE \
    --driver-memory $SPARK_DRIVER_MEMORY \
    --executor-memory $SPARK_EXECUTOR_MEMORY \
    --executor-cores $SPARK_EXECUTOR_CORES \
    Load_StrategyPartNo_TCUECGSYNC.py $DATABASE $CURRENT_USER $CRC4T_STRGYPARTNO_QUERY_DAYS $CURRENT_DATE

if [ "$?" != "0" ]; then
        echo "*** Load CRC4T StrategyPartNo Failed!!!"
        exit 1
fi

exit 0