#!/bin/bash

source /home/work/bin/zhaowei/machinealgorithm/bin/env.sh
DAYS=$1
if [ "x${DAYS}" == "x" ];then
    DAYS=1
fi

date=`date -d "-${DAYS}day" '+%Y%m%d'`

MISTAT_CUBE_PATH="/user/h_xiaomi_stats/zhaowei"
LOCAL_DATA_PATH="/home/work/data/zhaowei/linear"
mkdir -p ${LOCAL_DATA_PATH}

## 每日基础指标计算
hadoop --cluster ${hadoop_cluster} jar ${JAR} \
        com.xiaomi.stats.task.mapred.basic.MistatBasicMapReduce \
        -Dmapreduce.job.reduces=${reduce_num_min} \
        -Dmapreduce.job.queuename=root.production.miui_group.open_platform \
        /user/h_data_platform/platform/mistat/basic_middletier_merge/date=${date}/* \
        ${MISTAT_CUBE_PATH}/mistat_basic_cube/date=${date} \
        ${date}

## 结果写入HBase
sh ${BASE_DIR}/bin/basic/mistat_basic_stat_to_hbase.sh ${MISTAT_CUBE_PATH}/mistat_basic_cube/date=${date} ${date}

## 更新维度MySQL表
hadoop --cluster ${hadoop_cluster} fs -cat ${MISTAT_CUBE_PATH}/mistat_basic_cube/date=${date}/part-r-* > ${LOCAL_DATA_PATH}/mistat_basic_cube_${date}
java -Xms4096m -Xmx8192m -classpath ${JAR} \
        com.xiaomi.stats.task.mapred.basic.db.MistatBasicDimensionDBWriter \
        ${LOCAL_DATA_PATH}/mistat_basic_cube_${date}