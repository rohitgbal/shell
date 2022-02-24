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
TABLE=${13}
CURRENT_DATE=${14}

#kinit
kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

export SPARK_MAJOR_VERSION=2
spark-submit --master $SPARK_MASTER \
             --deploy-mode $SPARK_DEPLOY_MODE \
             --conf spark.dynamicAllocation.maxExecutors=$SPARK_MAX_EXECUTORS \
			 --conf spark.hadoop.metastore.catalog.default=hive \
			 --conf spark.driver.maxResultSize=8192m \
             --driver-memory $SPARK_DRIVER_MEMORY \
             --executor-memory $SPARK_EXECUTOR_MEMORY \
			 --executor-cores $SPARK_EXECUTOR_CORES \
			 --conf spark.sql.shuffle.partitions=1000 \
			 --files 4gscript.sql \
             loader.py --database $DATABASE --sql 4gscript.sql --user $USER --table $TABLE --output $OUTPUT --lookbacktable $DYS --currdate $CURRENT_DATE
			 

																																													
if [ "$?" != "0" ]; then
	 echo "*** WIL 4G Incremental Load Failed"
   exit 1
fi
exit 0																																													
