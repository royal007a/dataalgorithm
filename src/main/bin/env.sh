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

