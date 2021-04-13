#!/bin/bash
current_time=`date +"%Y-%m-%d %H:%M:%S"`
script=$(readlink -f $0)
script_dir=`cd \`dirname $0\`; pwd`
echo "-------$current_time--------"
echo "run script ${script}"
u_home="/home/work"
source ${u_home}/.bash_profile

source ${u_home}/code/configure/kafka.hdp.conf
source ${u_home}/code/configure/dingding.conf
source ${u_home}/code/configure/emp_mobile_info.conf
hdfs_jar=$script_dir/feature-spark-1.0.0.jar

cdate_hour=`date -d'-0 hour' +'%Y%m%d%H'`
yarn=yarn
jobtype="flink"
jobname="laplace-op-gen"
groupId="flink-${jobname}v4"
vs_group="listen-vidInfo-v20"
checkpoint_dir=hdfs:///user/flink/checkPoint/$groupId

echo "Check the job for ${jobtype}.${jobname} if running. Please wait..."
cnt1=$(${yarn} application -list|grep "${jobtype}.${jobname}"|wc -l)
sleep 1
echo ""
echo "Check the job for ${jobtype}.${jobname} if running again. Please wait..."
sleep 10
cnt2=$(${yarn} application -list|grep "${jobtype}.${jobname}"|wc -l)
if [ $cnt1 -ne 0 -o $cnt2 -ne 0 ];then
  echo "The job for ${jobtype}.${jobname} mabey is running. Please check it first."
  ${u_home}/code/kafka_scripts/h_kafka_est_lag.sh $groupId 450 $taoxiaofeng
  exit 0
fi

recover_chkp=`hdfs dfs -find "$checkpoint_dir" -name "_metadata" |sed 's/\/_metadata$//g'|awk -F'-' 'BEGIN{a=0}$NF-a>0{a=$NF;b=$0}END{print b}'`
if [[ -n "$recover_chkp" ]];then
  recover_param="-s $recover_chkp --allowNonRestoredState"
  echo recover from "$recover_param"
fi

cd $script_dir

echo $h_kafka_borkers

log_dir=/home/flink/log/$jobname
mkdir -p $log_dir
log_file=$log_dir/${jobname}-${cdate_hour}.log

class=com.rightpaddle.flink.LaplaceOpGen
parallelism=13

online_kafka="alybjf-online-kafka-v01.dns.rightpaddle.com:6667"
mysql_url="jdbc:mysql://rr-2zeb0000371ftl888.mysql.rds.aliyuncs.com:3306/video_upload"
username="bigdata_reader"
password="2WcZ5kh8uizgXsgbLZ7X"
redis=r-2zed3cb6d90f8888.redis.rds.aliyuncs.com/aaabbbccc

nohup  /usr/local/flink/bin/flink run -m yarn-cluster $recover_param \
  -p $parallelism  -yqu  flink -c ${class}  -ynm  "${jobtype}.${jobname}"   \
  $hdfs_jar \
  --kafka "${h_kafka_borkers}" \
  --group "$groupId" \
  --checkpoint "$checkpoint_dir" \
  --online_kafka "${online_kafka}" \
  --vs_group "${vs_group}" \
  --mysql_url "${mysql_url}" \
  --username "${username}" \
  --password "${password}" \
  --redis "${redis}" \
  --cache-minute 5 \
  --window-seconds 60 \
  --op-kafka "$h_kafka_borkers" --op-topic "laplace-feature-op-log" \
  --op-log-type update \
> $log_file 2>&1 &

unlink current.log
ln -s $log_file current.log

sleep 15
cnt=$(${yarn} application -list|grep "${jobtype}.${jobname}"|wc -l)
if [ $cnt -eq 0 ]; then
  title="Error---Yarn Job ${jobtype}.${jobname} is not running And Try restart failed. Please check!"
else
  title="WARNNING---Yarn Job ${jobtype}.${jobname} restart successful."
fi
/usr/bin/python  ${u_home}/code/configure/ding_robot_mobile.py "${streaming}" "${title}" $taoxiaofeng

