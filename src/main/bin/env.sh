#!/bin/bash

source /etc/profile 1>/dev/null 2>&1
export LANG="zh_CN.UTF-8"
export LC_ALL="zh_CN.UTF-8"
export JAVA_HOME=/home/work/soft/jdk
export PATH=${JAVA_HOME}/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/sbin:/home/work/opshell/:/home/work/bin:/home/work/soft/infra-client/bin:/home/work/tools/hive-0.13.1-cdh5.2.0/bin
set -x
BASE_DIR="/home/work/bin/xiaomi-stats-task"
MIDDLE_DATA_PATH="/user/h_xiaomi_stats/middle_data"
CUSTOM_MIDDLE_ROOT_PATH="/user/h_data_platform/platform/mistat/custom_middletier_merge"
USER_GROUP_MIDDLE_ROOT_PATH="/user/h_data_platform/platform/mistat/user_group_middletier_merge"
mistat_basic_after_mapping="/user/h_data_platform/platform/mistat/mistat_basic_after_mapping"
JAR="/home/work/bin/xiaomi-stats-task/xiaomi-stats-task-1.0-SNAPSHOT-jar-with-dependencies.jar"

get_zk_env() {
    local zk_env=$(cat /home/work/bin/xiaomi-stats-task/classes/zookeeper.properties  |sed -n 's/zookeeper.host=\(.*\)/\1/p')
    echo "$zk_env"
}

zk_env=$(get_zk_env)
if [ "$zk_env" = "c3" ]; then
    mistat_scribe_dir=/user/h_scribe/mistat
    basic_scribe_dir=${mistat_scribe_dir}/mistat_basic
    crash_middletier_dir=/user/h_xiaomi_stats/middle_data/crash_middletier
    dbhost_new="10.105.20.170"
    dbuser_new="root"
    dbpwd_new="83ssed3ks3sd9sue3"
    dbhost_new="10.105.10.202"
    dbuser="stats_wr"
    dbpwd="fb299b341349255d702e56b779c85b64"
    queue=production.miui_group.open_platform
    hbase_cluster="hbase://c3prc-xiaomi98/"
    hadoop_cluster="c302prc-adsctr"
    spark_env=" --conf spark.executor.heartbeatInterval=360s --conf spark.yarn.maxAppAttempts=3 --conf spark.yarn.jar=hdfs://c302prc-adsctr/spark/c302prc-adsctr/cache/spark-assembly-1.6.1-mdh1.6.1.6-hadoop2.6.0.jar "
    reduce_num_min=50
    reduce_num=1000
    reduce_num_max=3000
    
    executor_num_min=50
    executor_num=400
    executor_num_max=900
    
    spark_default_parallelism_min=100
    spark_default_parallelism=800
    spark_default_parallelism_max=1800
elif [ "$zk_env" = "aws_sgp" ]; then
    mistat_scribe_dir=/user/s_lcs/mistat
    basic_scribe_dir=${mistat_scribe_dir}/mistat_basic
    crash_middletier_dir=/user/h_data_platform/platform/mistat/crash_middletier
    dbhost="10.58.4.103"
    dbuser="mistat_xmstat_wr"
    dbpwd="b8f1b9d4aa719e98e2b1c558e81d609c"
    queue=root.production.miui_group.open_platform
    hbase_cluster="hbase://alsgprc-xiaomi/"
    hadoop_cluster="alsgprc-hadoop"
    spark_env=" --conf spark.executor.heartbeatInterval=360s --conf spark.yarn.maxAppAttempts=3 "
    
    reduce_num_min=10
    reduce_num=20
    reduce_num_max=50
    
    executor_num_min=10
    executor_num=10
    executor_num_max=20
    
    spark_default_parallelism_min=20
    spark_default_parallelism=20
    spark_default_parallelism_max=40
else
    mistat_scribe_dir=/user/h_scribe/mistat
    basic_scribe_dir=${mistat_scribe_dir}/mistat_basic
    crash_middletier_dir=/user/h_xiaomi_stats/middle_data/crash_middletier
    queue=default
    hbase_cluster="hbase://localhost:2181/"
    spark_env=" --conf spark.executor.heartbeatInterval=360s --conf spark.yarn.maxAppAttempts=3 "
    reduce_num_min=10
    reduce_num=20
    reduce_num_max=20
    
    executor_num_min=10
    executor_num=10
    executor_num_max=20
    
    spark_default_parallelism_min=20
    spark_default_parallelism=20
    spark_default_parallelism_max=40
fi

function monitor() {
    content=$1
    group=$2
    hadoop jar ${JAR} com.xiaomi.stats.task.monitor.MonitorData $content $group
}