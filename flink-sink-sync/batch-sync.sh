#!/bin/bash
if [ $# -lt 4 ]
then
        echo "参数输入不足，请输入：参数1[脱敏模式：mask/unmask] 参数2[数据库名称] 参数3[yyyy-MM-dd] 参数4[HH:mm:ss]"
        exit
fi

MASK_MODE=$1
DB=$2
DATE=$3
TIME=$4
echo "如运行错误，请将DbFullBatchSyncDoris设为主类后打包，开始执行......"
hadoop jar jobs/flink-batch-sync.jar ${MASK_MODE} ${DB} ${DATE} ${TIME}