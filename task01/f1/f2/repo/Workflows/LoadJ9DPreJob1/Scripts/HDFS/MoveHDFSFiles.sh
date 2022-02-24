#!/bin/bash
#*********************************************************************#
#*                                                                   *#
#*  Script Name    : MoveHDFSFiles.sh                                *#
#*  Purpose/Desc   : This script moves HDFS files along with         *#
#*                   directory structure recursively                 *#
#*                                                                   *#
#*  Revisions  :                                                     *#
#*     Date     CDS-ID   DESCRIPTION                                 *#
#*  ----------- --------  -----------------------------------------  *#
#*  2020-Jul-02 DREDDY25  Initial Version                            *#
#--------------------------------------------------------------------*#
#* Input Parameters:                                                 *#
#*  1  source Path                                                   *#
#*  2  target path                                                   *#
#*********************************************************************#

source_path=${1}
target_path=${2}
KERBEROS_PRINCIPAL=${3}
KERBEROS_KEYTAB_FILENAME=${4}

kinit -k -t ./$KERBEROS_KEYTAB_FILENAME $KERBEROS_PRINCIPAL

#* Check if there is source path has any files/dirs to move *#
#* if source path is empty, then return success             *#
#* else move files/directories to target path               *#
hdfs dfs -ls -R ${source_path} > tmp.tmp

isEmpty=$(ls -l tmp.tmp | awk '{print $5}')
if [[ $isEmpty -eq 0 ]];then
    rc=0
else
    hdfs dfs -mv ${source_path}/* ${target_path}/
    rc=$?
fi

if [[ $rc -ne 0 ]] ; then
    echo "Error while moving files from ${source_path} to ${target_path}. Error Code: " $rc
    exit -1
fi
exit 0
