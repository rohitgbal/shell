#!/bin/bash

DATABASE=${1}
CRC4T_J9D_PJ1_QUERY_DAYS=${2}
SPARK_MASTER=${3}
SPARK_NETWORK_TIMEOUT=${4}
SPARK_UI_PORT=${5}
SPARK_DEPLOY_MODE=${6}
SPARK_JARS=${7}
SPARK_DRIVER_MEMORY=${8}
SPARK_NUM_EXECUTORS=${9}
SPARK_EXECUTOR_MEMORY=${10}
OOZIE_QUEUE_NAME=${11}
SPARK_SQL_SHUFFLE_PARTITIONS=${12}
SPARK_SQL_ORC_FILTERPUSHDOWN=${13}
SPARK_YARN_EXECUTOR_MEMORY_OVERHEAD=${14}
SPARK_CORE_CONNECTION_ACK_WAIT_TIMEOUT=${15}
SPARK_TASK_MAXFAILURES=${16}
SPARK_SHUFFLE_IO_MAXRETRIES=${17}
SPARK_DYNAMICALLOCATION_ENABLED=${18}
SPARK_SQL_ORC_ENABLED=${19}
SPARK_SQL_HIVE_CONVERTMETASTOREORC=${20}
SPARK_SQL_ORC_CHAR_ENABLED=${21}
SPARK_FILES=${22}
KERBEROS_KEYTAB_FILENAME=${23}
KERBEROS_PRINCIPAL=${24}
OUTPUT=${25}


#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL


spark-submit --conf spark.hadoop.metastore.catalog.default=hive \
	     --conf spark.dynamicAllocation.maxExecutors=$SPARK_NUM_EXECUTORS \
             --master $SPARK_MASTER \
             --deploy-mode $SPARK_DEPLOY_MODE \
             --driver-memory $SPARK_DRIVER_MEMORY \
             --executor-memory $SPARK_EXECUTOR_MEMORY \
             --executor-cores 4 \
             Load_J9D_PJ1_StgFromCRC4T.py $DATABASE $CRC4T_J9D_PJ1_QUERY_DAYS $OUTPUT


if [ "$?" != "0" ]; then
        echo "*** Load CRC4T Vehicle Command/commandResponse to STG Failed!!!"
        exit 1
fi

exit 0

