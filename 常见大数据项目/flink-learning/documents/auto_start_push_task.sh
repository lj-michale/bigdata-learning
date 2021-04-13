#!/usr/bin/env bash
script=$(readlink -f $0)
cdate=$(date "+%Y-%m-%d %H:%M:%S")
chour=$(date "+%H")

flink_dir=/home/newapm/software/flink
log_name=auto_start_push_task.log
if [ -f ${flink_dir}/${log_name} ] && [ "${chour}" == "00" ]; then
  tail -n 5000 ${flink_dir}/${log_name} > ${flink_dir}/${log_name}.tmp
  \cp ${flink_dir}/${log_name}.tmp ${flink_dir}/${log_name}
  rm -rf ${flink_dir}/${log_name}.tmp
fi

echo ""
echo "-----------------------------------------------------------------------"
echo "Current DateTime: ${cdate}, begin run script ${script}"

flink_job_name=log_product_flink
softDir=/home/newapm/software
sleep_seconds=5
kafka_group_id=$(cat ${softDir}/flink/app/application.properties|grep -v "^#"|grep "group.id"|awk -F '=' '{print $2}')
kafka_topic=$(cat ${softDir}/flink/app/application.properties|grep -v "^#"|grep "kafka.read.topic"|awk -F '=' '{print $2}')
kafka_bootstrap_servers=$(cat ${softDir}/flink/app/application.properties|grep -v "^#"|grep "kafka.bootstrap.servers"|awk -F '=' '{print $2}')

sum_lag1=$(${softDir}/kafka/bin/kafka-consumer-groups.sh --describe --bootstrap-server ${kafka_bootstrap_servers} --group ${kafka_group_id}|grep "^${kafka_group_id} "|awk -v ktopic=$kafka_topic '{if($2==ktopic) {sum+=$6}} END {print sum}')
sleep ${sleep_seconds}
sum_lag2=$(${softDir}/kafka/bin/kafka-consumer-groups.sh --describe --bootstrap-server ${kafka_bootstrap_servers} --group ${kafka_group_id}|grep "^${kafka_group_id} "|awk -v ktopic=$kafka_topic '{if($2==ktopic) {sum+=$6}} END {print sum}')
sleep ${sleep_seconds}
sum_lag3=$(${softDir}/kafka/bin/kafka-consumer-groups.sh --describe --bootstrap-server ${kafka_bootstrap_servers} --group ${kafka_group_id}|grep "^${kafka_group_id} "|awk -v ktopic=$kafka_topic '{if($2==ktopic) {sum+=$6}} END {print sum}')
sleep ${sleep_seconds}
sum_lag4=$(${softDir}/kafka/bin/kafka-consumer-groups.sh --describe --bootstrap-server ${kafka_bootstrap_servers} --group ${kafka_group_id}|grep "^${kafka_group_id} "|awk -v ktopic=$kafka_topic '{if($2==ktopic) {sum+=$6}} END {print sum}')
sleep ${sleep_seconds}
sum_lag5=$(${softDir}/kafka/bin/kafka-consumer-groups.sh --describe --bootstrap-server ${kafka_bootstrap_servers} --group ${kafka_group_id}|grep "^${kafka_group_id} "|awk -v ktopic=$kafka_topic '{if($2==ktopic) {sum+=$6}} END {print sum}')

echo "sumLag1: ${sum_lag1}, sumLag2: ${sum_lag2}, sumLag3: ${sum_lag3}, sumLag4: ${sum_lag4}, sumLag5: ${sum_lag5}"

if  [ ${sum_lag5} -gt 100000 ] && [ ${sum_lag5} -gt ${sum_lag4} ] && [ ${sum_lag4} -gt ${sum_lag3} ] && [ ${sum_lag3} -gt ${sum_lag2} ] && [ ${sum_lag2} -gt ${sum_lag1} ]; then
  echo "Lag is increment, will restart flink..."
  # get flink job id, if not exists force restart
  is_running=$(${softDir}/flink/bin/flink list --running|grep "${flink_job_name}"|wc -l)
  if [ ${is_running} -ge 1 ]; then
     echo "Flink job ${flink_job_name} is running, will cancel and commit job, Please wait..."
     sleep 1
     job_id=$(${softDir}/flink/bin/flink list --running|grep "${flink_job_name}"|awk -F ' : ' '{print $2}')
     echo "Flink Running jobId: ${job_id},will be cansel and reCommit..."
     sleep 1
     ${softDir}/flink/bin/flink cancel ${job_id}
  else 
    echo "Flink job ${flink_job_name} not running, will restart flink cluster and commit job, Please wait..."
    sleep 1;
  fi
  sleep 3;
  ${softDir}/flink/bin/stop-cluster.sh; sleep 2; ${softDir}/flink/bin/stop-cluster.sh;
  ${softDir}/flink/bin/start-cluster.sh; sleep 5;
  cd ${softDir}/flink/; ${softDir}/flink/push_task.sh; sleep 5; 
  
  # check if push task succefful
  is_running=$(${softDir}/flink/bin/flink list --running|grep "${flink_job_name}"|wc -l)
  if [ ${is_running} -ge 1 ]; then
    echo "Flink job reCommit succefflu,Good JOb"
  else
    echo "Flink job reCommit failed,Please check!!!"
  fi
else
  echo "running ok"
fi

cdate=$(date "+%Y-%m-%d %H:%M:%S")
echo "Current DateTime: ${cdate}, end run script ${script}"
echo "-----------------------------------------------------------------------"
echo ""
