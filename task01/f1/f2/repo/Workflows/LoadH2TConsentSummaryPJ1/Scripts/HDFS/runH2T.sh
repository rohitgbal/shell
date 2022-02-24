#!/bin/bash

SPARK_MASTER=${1}
SPARK_DEPLOY_MODE=${2}
SPARK_MAX_EXECUTORS=${3}
SPARK_DRIVER_MEMORY=${4}
SPARK_EXECUTOR_MEMORY=${5}
SPARK_EXECUTOR_CORES=${6}
OUTPUT=${7}
DATABASE=${8}
KERBEROS_KEYTAB_FILENAME=${9}
KERBEROS_PRINCIPAL=${10}
USER=${11}
DYS=${12}
RAWTABLE=${13}
J9DTABLE=${14}
H2TTABLE=${15}
CURRENT_DATE=${16}

#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export SPARK_MAJOR_VERSION=2
spark-submit --master $SPARK_MASTER \
             --deploy-mode $SPARK_DEPLOY_MODE \
             --conf spark.dynamicAllocation.maxExecutors=$SPARK_MAX_EXECUTORS \
			 --conf spark.hadoop.metastore.catalog.default=hive \
			 --conf spark.yarn.executor.memoryOverhead=6g \
			 --conf spark.driver.maxResultSize=6g \
			 --conf spark.sql.hive.convertMetastoreOrc='true' \
		     --conf spark.sql.orc.char.enabled='true' \
		     --conf spark.sql.orc.filterPushdown='true' \
			 --conf spark.sql.orc.enabled='true' \
			 --conf spark.network.timeout=1200 \
			 --conf spark.sql.broadcastTimeout=1000 \
			 --conf spark.sql.tungsten.enabled='false' \
			 --conf spark.shuffle.spillAfterRead='true' \
			 --conf spark.executor.extraJavaOptions='-XX:+UseNUMA -XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=12 -XX:+UseStringDeduplication' \
			 --conf spark.default.parallelism=500 \
			 --conf spark.driver.extraJavaOptions=-Duser.timezone="UTC" \
			 --conf spark.yarn.max.executor.failures=200 \
			 --conf spark.task.maxFailures=8 \
			 --conf spark.dynamicAllocation.executorIdleTimeout=1000 \
			 --driver-memory $SPARK_DRIVER_MEMORY \
             --executor-memory $SPARK_EXECUTOR_MEMORY \
			 --executor-cores $SPARK_EXECUTOR_CORES \
			 --conf spark.sql.shuffle.partitions=5000 \
			 --conf spark.executor.extraJavaOptions=-Duser.timezone=UTC \
			 --jars /usr/hdp/current/hive_warehouse_connector/hive-warehouse-connector-assembly-* \
			 --py-files /usr/hdp/current/hive_warehouse_connector/pyspark_hwc-* \
			 H2Ttransform.py --database $DATABASE --user $USER --rawtable $RAWTABLE --J9Dtable $J9DTABLE --H2Ttable $H2TTABLE --output $OUTPUT --lookbacktable $DYS --currdate $CURRENT_DATE
																				
if [ "$?" != "0" ]; then
	 echo "*** H2T 4G Incremental Load Failed"
   exit 1
fi
exit 0																																													
