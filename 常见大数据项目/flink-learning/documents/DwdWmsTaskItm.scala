package com.ztocwst.flink.dwd


import java.util.{Optional, Properties}
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.{JSON, JSONObject, TypeReference}
import com.zto.fire.common.util.PropUtils
import com.zto.fire.demo.bean.MaxwellDataFormat
import com.ztocwst.flink.dwd.bean._
import org.apache.flink.api.scala._
import com.ztocwst.flink.util.misc._
import com.ztocwst.flink.CWSTFlinkStreaming
import com.ztocwst.flink.dwd.tools.{KafkaKeySchema, KafkaPartitionSchema}
import com.ztocwst.flink.util.const.TableSql
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * TODO
  * @Created by
  * @Date 2020-08-15 18:38
  */
object DwdWmsTaskItm extends CWSTFlinkStreaming{

  val taskDetailTableName = "task_detail"
  val taskHeaderTableName = "task_header"
  val srrTableName = "second_review_record"
  val ccdTableName = "cycle_count_detail"
  val ovsTableName = "operate_volume_statistics"
  val printingHistoryTableName = "printing_history"
  val waveTableName = "wave"


  override val stateRetentionMinTime = 15
  override val stateRetentionMxnTime = 18
  override val parallelism = 5

  val detailTopic = "task_detail"
  val othersTopic = "wms_others"
  val detailGroupId = "task_detail_DwdWmsTaskItm_group"
  val othersGroupId = "wms_others_DwdWmsTaskItm_group"
  val sinkTopic = "dwd_wms_task_itm"



  override def process: Unit = {

    //开启Mini-Batch聚合
    val configuration = tEnv.getConfig().getConfiguration()
    configuration.setString("table.exec.mini-batch.enabled", "true")
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s")
    configuration.setString("table.exec.mini-batch.size", "5000")

    // kafka 数据源
    val detailPro = new Properties()
    detailPro.setProperty("bootstrap.servers", bootstrapServers)
    detailPro.setProperty("group.id",detailGroupId)
    val othersPro = new Properties()
    othersPro.setProperty("bootstrap.servers", bootstrapServers)
    othersPro.setProperty("group.id",othersGroupId)
    val startLongTime = getLongTime(kafkaStartTime)

    val detail_source = new FlinkKafkaConsumer011(detailTopic, new SimpleStringSchema(), detailPro)
      .setStartFromTimestamp(startLongTime)
    val others_source = new FlinkKafkaConsumer011(othersTopic, new SimpleStringSchema(), othersPro)
      .setStartFromTimestamp(startLongTime)
    val detailSrc = env.addSource(detail_source)
    val othersSrc = env.addSource(others_source)

    val othersStream = othersSrc.map(line => {
      val data = JSON.parseObject(line, new TypeReference[MaxwellDataFormat]() {})
      data
    }).filter(_.getTable != null)
    val headerSrc = othersStream.filter(_.getTable.equals(taskHeaderTableName))
    val srrSrc = othersStream.filter(_.getTable.equals(srrTableName))
    val ccdSrc = othersStream.filter(_.getTable.equals(ccdTableName))
    val ovsSrc = othersStream.filter(_.getTable.equals(ovsTableName))
    val phSrc = othersStream.filter(_.getTable.equals(printingHistoryTableName))
    val waveSrc = othersStream.filter(_.getTable.equals(waveTableName))



    // 解析数据
    val detailStream = detailSrc.map(line =>{
      val data = JSON.parseObject(line, new TypeReference[MaxwellDataFormat]() {})
      data
    }).filter(_.getData != null).filter(_.getData.length > 30).map(data =>{
      //解析数据
      val detail_data = JSON.parseObject(data.getData, new TypeReference[TaskDetail]() {})
      detail_data.tableName = data.getTable
      //时间戳
//      detail_data.mysqlposition = data.getPosition.split("\\.")(1).split(":")(0) +
//        data.getPosition.split("\\.")(1).split(":")(1)
      //数据状态
      detail_data.row_type = if (data.getType.length > 10) "insert" else data.getType
      //货主编码
      if (detail_data.companyCode != null && !detail_data.companyCode.equals("") && detail_data.companyCode != None) {
        detail_data.companyCode = detail_data.companyCode.trim.toUpperCase
      } else {
        detail_data.companyCode = ""
      }
      //仓库编码
      if (detail_data.warehouseCode != null && !detail_data.warehouseCode.equals("") && detail_data.warehouseCode != None) {
        detail_data.warehouseCode = detail_data.warehouseCode.trim.toUpperCase
      } else {
        detail_data.warehouseCode = ""
      }
      detail_data
    }).filter(x => {
      !("delete".equals(x.row_type) && (x.status.toInt > 100) ) &&
        // 过滤归档和冷数据
        x.tableName.equals(taskDetailTableName)
    })

    val headerStream = headerSrc.map(binlog => {
      val header_data = JSON.parseObject(binlog.getData, new TypeReference[TaskHeader]() {})
      //时间戳
      header_data.mysqlposition = binlog.getPosition.split("\\.")(1).split(":")(0) +
        binlog.getPosition.split("\\.")(1).split(":")(1)
      header_data.tableName = binlog.getTable
      //数据状态
      header_data.row_type = if (binlog.getType.length > 10) "insert" else binlog.getType
      //货主编码
      if (header_data.companyCode != null && !header_data.companyCode.equals("") && header_data.companyCode != None) {
        header_data.companyCode = header_data.companyCode.trim.toUpperCase
      } else {
        header_data.companyCode = ""
      }
      //仓库编码
      if (header_data.warehouseCode != null && !header_data.warehouseCode.equals("") && header_data.warehouseCode != None) {
        header_data.warehouseCode = header_data.warehouseCode.trim.toUpperCase
      } else {
        header_data.warehouseCode = ""
      }
      header_data
    }).filter(x => {
      !("delete".equals(x.row_type) && (x.status.toInt > 100) ) &&
        // 过滤归档和冷数据
        x.tableName.equals(taskHeaderTableName)
    })

    val ssrStream = srrSrc.map(binlog => {
      //解析数据
      val srr_data = JSON.parseObject(binlog.getData, new TypeReference[SecondReviewRecord]() {})
      //时间戳
//      srr_data.mysqlposition = binlog.getPosition.split("\\.")(1).split(":")(0) +
//        binlog.getPosition.split("\\.")(1).split(":")(1)
      //数据状态
      srr_data.row_type = binlog.getType
      //货主编码
      if (srr_data.companyCode != null && !srr_data.companyCode.equals("") && srr_data.companyCode != None) {
        srr_data.companyCode = srr_data.companyCode.trim.toUpperCase
      } else {
        srr_data.companyCode = ""
      }
      //仓库编码
      if (srr_data.warehouseCode != null && !srr_data.warehouseCode.equals("") && srr_data.warehouseCode != None) {
        srr_data.warehouseCode = srr_data.warehouseCode.trim.toUpperCase
      } else {
        srr_data.warehouseCode = ""
      }
      //承运商编码
      if (srr_data.carrierCode != null && !srr_data.carrierCode.equals("") && srr_data.carrierCode != None) {
        srr_data.carrierCode = srr_data.carrierCode.trim.toUpperCase
      } else {
        srr_data.carrierCode = ""
      }
      srr_data
    })

    val ccdStream = ccdSrc.map(binlog => {
      //解析数据
      val ccd_data = JSON.parseObject(binlog.getData, new TypeReference[CycleCountDetail]() {})
      //时间戳
      ccd_data.mysqlposition = binlog.getPosition.split("\\.")(1).split(":")(0) +
        binlog.getPosition.split("\\.")(1).split(":")(1)
      //数据状态
      ccd_data.row_type = binlog.getType
      //货主编码
      if (ccd_data.companyCode != null && !ccd_data.companyCode.equals("") && ccd_data.companyCode != None) {
        ccd_data.companyCode = ccd_data.companyCode.trim.toUpperCase
      } else {
        ccd_data.companyCode = ""
      }
      //仓库编码
      if (ccd_data.warehouseCode != null && !ccd_data.warehouseCode.equals("") && ccd_data.warehouseCode != None) {
        ccd_data.warehouseCode = ccd_data.warehouseCode.trim.toUpperCase
      } else {
        ccd_data.warehouseCode = ""
      }
      ccd_data
    })

    val ovsStream = ovsSrc.map(binlog => {
      //解析数据
      val ovs_data = JSON.parseObject(binlog.getData, new TypeReference[OperateVolumeStatistics]() {})
      //时间戳
      ovs_data.mysqlposition = binlog.getPosition.split("\\.")(1).split(":")(0) +
        binlog.getPosition.split("\\.")(1).split(":")(1)
      //数据状态
      ovs_data.row_type = binlog.getType
      //仓库编码
      if (ovs_data.warehouseCode != null && !ovs_data.warehouseCode.equals("") && ovs_data.warehouseCode != None) {
        ovs_data.warehouseCode = ovs_data.warehouseCode.trim.toUpperCase
      } else {
        ovs_data.warehouseCode = ""
      }
      ovs_data
    })

    val phStream = phSrc.map(binlog => {
      //解析数据
      val ph_data = JSON.parseObject(binlog.getData, new TypeReference[PrintingHistory]() {})
      //时间戳
      ph_data.mysqlposition = binlog.getPosition.split("\\.")(1).split(":")(0) +
        binlog.getPosition.split("\\.")(1).split(":")(1)
      //数据状态
      ph_data.row_type = binlog.getType
      //仓库编码
      if (ph_data.warehouseCode != null && !ph_data.warehouseCode.equals("") && ph_data.warehouseCode != None) {
        ph_data.warehouseCode = ph_data.warehouseCode.trim.toUpperCase
      } else {
        ph_data.warehouseCode = ""
      }
      ph_data
    })

    val waveStream =  waveSrc.map(binlog => {
      //解析数据
      val wave_data = JSON.parseObject(binlog.getData, new TypeReference[Wave]() {})
      //时间戳
      wave_data.mysqlposition = (binlog.getPosition.split("\\.")(1).split(":")(0) +
        binlog.getPosition.split("\\.")(1).split(":")(1)).toLong
      //数据状态
      wave_data.row_type = binlog.getType
      //仓库编码
      if (wave_data.warehouseCode != null && !wave_data.warehouseCode.equals("") && wave_data.warehouseCode != None) {
        wave_data.warehouseCode = wave_data.warehouseCode.trim.toUpperCase
      } else {
        wave_data.warehouseCode = ""
      }
      wave_data
    })//.filter(_.row_type.equals("insert"))


    // sql
    tEnv.createTemporaryView("task_detail",detailStream)
    tEnv.createTemporaryView("task_header",headerStream)
    tEnv.createTemporaryView("second_review_record",ssrStream)
    tEnv.createTemporaryView("cycle_count_detail",ccdStream)
    tEnv.createTemporaryView("operate_volume_statistics",ovsStream)
    tEnv.createTemporaryView("printing_history",phStream)
    tEnv.createTemporaryView("wave",waveStream)

    tEnv.sqlUpdate(TableSql.getCreateWarehouseDimSql())
    tEnv.sqlUpdate(TableSql.getCreateCompanyDimSql())
    tEnv.sqlUpdate(TableSql.getCreateTtxUserDimSql())
    tEnv.sqlUpdate(TableSql.getCreateCarrierDimSql())





    val task_detail_view = tEnv.sqlQuery(
      """
         |select id,warehouseCode,companyCode,
         |last_value(taskId) as taskId,
         |last_value(itemCode) as itemCode,
         |last_value(taskCode) as taskCode,
         |last_value(taskType) as taskType,
         |last_value(itemName) as itemName,
         |last_value(totalQty) as totalQty,
         |last_value(fromQty) as fromQty,
         |last_value(toQty) as toQty,
         |last_value(fromLoc) as fromLoc,
         |last_value(toLoc) as toLoc,
         |last_value(fromLPN) as fromLPN,
         |last_value(toLPN) as toLPN,
         |last_value(fromZone) as fromZone,
         |last_value(toZone) as toZone,
         |last_value(currentLoc) as currentLoc,
         |last_value(currentLPN) as currentLPN,
         |last_value(attributeId) as attributeId,
         |last_value(status) as status,
         |last_value(referenceCode) as referenceCode,
         |last_value(referenceId) as referenceId,
         |last_value(referenceLineId) as referenceLineId,
         |last_value(referenceContCode) as referenceContCode,
         |last_value(referenceContId) as referenceContId,
         |last_value(assignedUser) as assignedUser,
         |last_value(internalTaskType) as internalTaskType,
         |last_value(batch) as batch,
         |last_value(lot) as lot,
         |last_value(manufactureDate) as manufactureDate,
         |last_value(expirationDate) as expirationDate,
         |last_value(agingDate) as agingDate,
         |last_value(inventorySts) as inventorySts,
         |last_value(waveId) as waveId,
         |last_value(referenceReqId) as referenceReqId,
         |last_value(fromInventoryId) as fromInventoryId,
         |last_value(toInventoryId) as toInventoryId,
         |last_value(confirmedBy) as confirmedBy,
         |last_value(pickingCartCode) as pickingCartCode,
         |last_value(transContCode) as transContCode,
         |last_value(pickingCartPos) as pickingCartPos,
         |last_value(pickDropLoc) as pickDropLoc,
         |last_value(rebinShortQty) as rebinShortQty,
         |last_value(groupNum) as groupNum,
         |last_value(groupIndex) as groupIndex,
         |last_value(created) as created,
         |last_value(createdBy) as createdBy,
         |last_value(lastUpdated) as lastUpdated,
         |last_value(lastUpdatedBy) as lastUpdatedBy,
         |last_value(version) as version,
         |last_value(processStamp) as processStamp,
         |last_value(csQty) as csQty,
         |last_value(plQty) as plQty,
         |last_value(inTransitLocked) as inTransitLocked,
         |last_value(boxCode) as boxCode,
         |last_value(positionNums) as positionNums,
         |last_value(row_type) as row_type,
         |last_value(warehouse_name) as warehouse_name,
         |last_value(company_name) as company_name,
         |last_value(warehouse_region) as warehouse_region,
         |last_value(warehouse_type) as warehouse_type,
         |last_value(warehouse_state) as warehouse_state,
         |last_value(warehouse_city) as warehouse_city,
         |last_value(warehouse_district) as warehouse_district,
         |last_value(assigned_name) as assigned_name,
         |last_value(department) as department,
         |PROCTIME() AS proctime
         |from task_detail
         |group by id,warehouseCode,companyCode
      """.stripMargin)
    val task_header_view = tEnv.sqlQuery(
      s"""
         |select id,warehouseCode,companyCode,
         |last_value(code) as code,
         |last_value(taskType) as taskType,
         |last_value(internalTaskType) as internalTaskType,
         |last_value(referenceId) as referenceId,
         |last_value(referenceCode) as referenceCode,
         |last_value(assignedUser) as assignedUser,
         |last_value(confirmedBy) as confirmedBy,
         |last_value(status) as status,
         |last_value(holdCode) as holdCode,
         |last_value(waveId) as waveId,
         |last_value(pickingCartCode) as pickingCartCode,
         |last_value(pickingCartPos) as pickingCartPos,
         |last_value(transContId) as transContId,
         |last_value(startPickDateTime) as startPickDateTime,
         |last_value(endPickDateTime) as endPickDateTime,
         |last_value(rebatchLoc) as rebatchLoc,
         |last_value(finishRebatch) as finishRebatch,
         |last_value(rebatchGroupCode) as rebatchGroupCode,
         |last_value(allowRebatch) as allowRebatch,
         |last_value(taskProcessType) as taskProcessType,
         |last_value(rebinBench) as rebinBench,
         |last_value(rebined) as rebined,
         |last_value(startRebinDateTime) as startRebinDateTime,
         |last_value(endRebinDateTime) as endRebinDateTime,
         |last_value(rebinedBy) as rebinedBy,
         |last_value(exceptionCode) as exceptionCode,
         |last_value(exceptionHandledBy) as exceptionHandledBy,
         |last_value(created) as created,
         |last_value(createdBy) as createdBy,
         |last_value(lastUpdated) as lastUpdated,
         |last_value(lastUpdatedBy) as lastUpdatedBy,
         |last_value(version) as version,
         |last_value(processStamp) as processStamp,
         |last_value(row_type) as row_type
         |from task_header
         |group by id,warehouseCode,companyCode
       """.stripMargin)
    val srr_view = tEnv.sqlQuery(
      """
        |select id,warehouseCode,companyCode,
        |last_value(samplingStatus) samplingStatus,
        |last_value(expressCode) expressCode,
        |last_value(waveId) waveId,
        |last_value(shipmentCode) shipmentCode,
        |last_value(created) created,
        |last_value(createdBy) createdBy,
        |last_value(created_name) created_name,
        |last_value(lastUpdated) lastUpdated,
        |last_value(lastUpdatedBy) lastUpdatedBy,
        |last_value(last_updated_name) last_updated_name,
        |last_value(version) version,
        |last_value(carrierCode) carrierCode,
        |last_value(warehouse_name) warehouse_name,
        |last_value(company_name) company_name,
        |last_value(warehouse_region) warehouse_region,
        |last_value(warehouse_type) warehouse_type,
        |last_value(warehouse_state) warehouse_state,
        |last_value(warehouse_city) warehouse_city,
        |last_value(warehouse_district) warehouse_district,
        |last_value(carrier_name) carrier_name,
        |last_value(row_type) row_type,
        |PROCTIME() AS proctime
        |from second_review_record
        |group by id,warehouseCode,companyCode
      """.stripMargin)
    val ccd_view = tEnv.sqlQuery(
      """
        |select id,warehouseCode,companyCode,
        |last_value(countId) countId,
        |last_value(round) round,
        |last_value(groupNum) groupNum,
        |last_value(groupIndex) groupIndex,
        |last_value(locationCode) locationCode,
        |last_value(parentLpn) parentLpn,
        |last_value(lpn) lpn,
        |last_value(itemCode) itemCode,
        |last_value(itemName) itemName,
        |last_value(inventorySts) inventorySts,
        |last_value(systemQty) systemQty,
        |last_value(countedQty) countedQty,
        |last_value(countedBy) countedBy,
        |last_value(count_name) count_name,
        |last_value(countedAt) countedAt,
        |last_value(assignedTo) assignedTo,
        |last_value(assignedAt) assignedAt,
        |last_value(completedBy) completedBy,
        |last_value(completedAt) completedAt,
        |last_value(status) status,
        |last_value(created) created,
        |last_value(createdBy) createdBy,
        |last_value(lastUpdated) lastUpdated,
        |last_value(lastUpdatedBy) lastUpdatedBy,
        |last_value(version) version,
        |last_value(processStamp) processStamp,
        |last_value(adjustQty) adjustQty,
        |last_value(rejectionNote) rejectionNote,
        |last_value(csQty) csQty,
        |last_value(plQty) plQty,
        |last_value(batch) batch,
        |last_value(lot) lot,
        |last_value(manufactureDate) manufactureDate,
        |last_value(expirationDate) expirationDate,
        |last_value(agingDate) agingDate,
        |last_value(cycleCode) cycleCode,
        |last_value(warehouse_name) warehouse_name,
        |last_value(company_name) company_name,
        |last_value(warehouse_region) warehouse_region,
        |last_value(warehouse_type) warehouse_type,
        |last_value(warehouse_state) warehouse_state,
        |last_value(warehouse_city) warehouse_city,
        |last_value(warehouse_district) warehouse_district,
        |last_value(row_type) row_type,
        |PROCTIME() AS proctime
        |from cycle_count_detail
        |group by id,warehouseCode,companyCode
      """.stripMargin)
    val ovs_view = tEnv.sqlQuery(
      """
        |select id,warehouseCode,
        |last_value(code) code,
        |last_value(operateLeader) operateLeader,
        |last_value(operateLeaderName) operateLeaderName,
        |last_value(operateLinkCode) operateLinkCode,
        |last_value(operateLinkName) operateLinkName,
        |last_value(operatorNum) operatorNum,
        |last_value(finishQtyUom) finishQtyUom,
        |last_value(taskNo) taskNo,
        |last_value(finishOrderNum) finishOrderNum,
        |last_value(finishTime) finishTime,
        |last_value(operateShift) operateShift,
        |last_value(version) version,
        |last_value(created) created,
        |last_value(createdBy) createdBy,
        |last_value(lastUpdated) lastUpdated,
        |last_value(lastUpdatedBy) lastUpdatedBy,
        |last_value(warehouse_name) warehouse_name,
        |last_value(warehouse_region) warehouse_region,
        |last_value(warehouse_type) warehouse_type,
        |last_value(warehouse_state) warehouse_state,
        |last_value(warehouse_city) warehouse_city,
        |last_value(warehouse_district) warehouse_district,
        |last_value(row_type) row_type,
        |PROCTIME() AS proctime
        |from operate_volume_statistics
        |group by id,warehouseCode
      """.stripMargin)
    val ph_view = tEnv.sqlQuery(
      """
        |select id,warehouseCode,
        |last_value(reportId) reportId,
        |last_value(reportCode) reportCode,
        |last_value(params) params,
        |last_value(printedAt) printedAt,
        |last_value(printedBy) printedBy,
        |last_value(printed_name) printed_name,
        |last_value(clientInfo) clientInfo,
        |last_value(warehouse_name) warehouse_name,
        |last_value(warehouse_region) warehouse_region,
        |last_value(warehouse_type) warehouse_type,
        |last_value(warehouse_state) warehouse_state,
        |last_value(warehouse_city) warehouse_city,
        |last_value(warehouse_district) warehouse_district,
        |last_value(row_type) row_type,
        |PROCTIME() AS proctime
        |from printing_history
        |group by id,warehouseCode
      """.stripMargin)

    val wave_view = tEnv.sqlQuery(
      """
        |select warehouseCode,
        |id,
        |max(waveType) waveType,
        |max(completedAt) completedAt,
        |max(mysqlposition) as mysqlposition
        |from wave
        |group by warehouseCode,id
        |""".stripMargin)


    env.setParallelism(10)
    val task_view = tEnv.sqlQuery(
      s"""
        |select
        |a.id as id,
        |b.id as task_id,
        |b.warehouseCode as warehouse_code,
        |b.companyCode as company_code,
        |b.code as task_code,
        |case when b.taskType in ('批量拣选','按单拣选') then
        |   case when wa.waveType='1' then '按单拣选' when wa.waveType='2' then '批量拣选' else '' end
        |else b.taskType end as task_typ,
        |b.internalTaskType as internal_task_typ,
        |a.referenceCode as rcp_code,
        |a.referenceId as rcp_id,
        |a.status as task_sts,
        |b.startPickDateTime as start_pick_time,
        |b.endPickDateTime as end_pick_time,
        |COALESCE(b.waveId,'-1') as wave_id,
        |b.rebined as nd_pick_sts,
        |b.startRebinDateTime as nd_start_pick_time,
        |b.endRebinDateTime as nd_end_pick_time,
        |b.rebinedBy as nd_pick_by,
        |b.created as created,
        |a.itemCode as itm_code,
        |a.itemName as itm_name,
        |a.totalQty as itm_qty,
        |a.fromQty as itm_from_qty,
        |a.toQty as itm_to_qty,
        |a.fromLoc as itm_from_loc,
        |a.toLoc as itm_to_loc,
        |a.fromZone as itm_from_zone,
        |a.toZone as itm_to_zone,
        |a.currentLoc as current_loc,
        |a.currentLPN as plt_current,
        |a.referenceContCode as ctn_code,
        |a.fromInventoryId as from_stk_code,
        |a.toInventoryId as to_stk_code,
        |a.csQty as ctn_convert_cnt,
        |a.plQty as plt_convert_cnt,
        |a.assignedUser as assigned_to,
        |b.confirmedBy as confirm_by,
        |w.warehouse_name as warehouse_name,
        |w.warehouse_typ as warehouse_type,
        |w.warehouse_region as warehouse_region,
        |c.company_name as company_name,
        |u.name as assigned_name,
        |u.department as assigned_dept,
        |a.referenceContId as ctn_id,
        |w.warehouse_pvc as warehouse_state,
        |a.row_type as row_type,
        |a.lastUpdated as lastupdated,
        |a.version as version,
        |a.lastUpdatedBy as lastupdatedby,
        |a.createdBy as createdby,
        |'' as samplingsts
        |from $task_detail_view a
        |join $task_header_view b on a.taskId = b.id and a.warehouseCode=b.warehouseCode and a.companyCode=b.companyCode
        |left join dim_warehouse FOR SYSTEM_TIME AS OF a.proctime as w on a.warehouseCode = w.warehouse_code
        |left join dim_company FOR SYSTEM_TIME AS OF a.proctime as c on a.warehouseCode = c.warehouse_code and a.companyCode = c.company_code
        |left join dim_ttx_user FOR SYSTEM_TIME AS OF a.proctime as u on a.assignedUser = u.code
        |left join $wave_view wa on wa.id = b.waveId and wa.warehouseCode = b.warehouseCode
        |union all
        |select
        |id,
        |'' as task_id,
        |a.warehouseCode as warehouse_code,
        |a.companyCode as company_code,
        |b.taskCode as task_code,
        |_UTF16'二次复核' as task_typ,
        |'' as internal_task_typ,
        |a.shipmentCode as rcp_code,
        |'' as rcp_id,
        |'' as task_sts,
        |created as start_pick_time,
        |lastUpdated as end_pick_time,
        |COALESCE(waveId,'-1') as wave_id,
        |'' as nd_pick_sts,
        |'' as nd_start_pick_time,
        |'' as nd_end_pick_time,
        |'' as nd_pick_by,
        |created as created,
        |'' as itm_code,
        |'' as itm_name,
        |'' as itm_qty,
        |'' as itm_from_qty,
        |'' as itm_to_qty,
        |'' as itm_from_loc,
        |'' as itm_to_loc,
        |'' as itm_from_zone,
        |'' as itm_to_zone,
        |'' as current_loc,
        |'' as plt_current,
        |'' as ctn_code,
        |'' as from_stk_code,
        |'' as to_stk_code,
        |'' as ctn_convert_cnt,
        |'' as plt_convert_cnt,
        |createdBy as assigned_to,
        |lastUpdatedBy as confirm_by,
        |w.warehouse_name as warehouse_name,
        |w.warehouse_typ as warehouse_type,
        |w.warehouse_region as warehouse_region,
        |c.company_name as company_name,
        |u.name as assigned_name,
        |'' as assigned_dept,
        |'' as ctn_id,
        |w.warehouse_pvc as warehouse_state,
        |row_type,
        |lastUpdated as lastupdated,
        |version as version,
        |lastUpdatedBy as lastupdatedby,
        |createdBy as createdby,
        |case when samplingStatus = '1' then '通过' else '未通过' end as samplingsts
        |from $srr_view a
        |join
        |(select
        |last_value(taskCode) as taskCode,
        |referenceCode,
        |warehouseCode,
        |companyCode
        |from task_detail
        |group by
        |referenceCode,
        |warehouseCode,
        |companyCode
        |)b
        |on a.shipmentCode = b.referenceCode and a.warehouseCode = b.warehouseCode and a.companyCode = b.companyCode
        |left join dim_warehouse FOR SYSTEM_TIME AS OF a.proctime as w on a.warehouseCode = w.warehouse_code
        |left join dim_company FOR SYSTEM_TIME AS OF a.proctime as c on a.warehouseCode = c.warehouse_code and a.companyCode = c.company_code
        |left join dim_ttx_user FOR SYSTEM_TIME AS OF a.proctime as u on a.lastUpdatedBy = u.code
        |union all
        |select
        |id,
        |countId as task_id,
        |warehouseCode as warehouse_code,
        |companyCode as company_code,
        |groupNum as task_code,
        |_UTF16'盘点' as task_typ,
        |'' as internal_task_typ,
        |'' as rcp_code,
        |'' as rcp_id,
        |a.status as task_sts,
        |countedAt as start_pick_time,
        |countedAt as end_pick_time,
        |'-1' as wave_id,
        |'' as nd_pick_sts,
        |'' as nd_start_pick_time,
        |'' as nd_end_pick_time,
        |'' as nd_pick_by,
        |created as created,
        |itemCode as itm_code,
        |itemName as itm_name,
        |countedQty as itm_qty,
        |'' as itm_from_qty,
        |'' as itm_to_qty,
        |'' as itm_from_loc,
        |'' as itm_to_loc,
        |'' as itm_from_zone,
        |'' as itm_to_zone,
        |locationCode as current_loc,
        |lpn as plt_current,
        |'' as ctn_code,
        |'' as from_stk_code,
        |'' as to_stk_code,
        |csQty as ctn_convert_cnt,
        |plQty as plt_convert_cnt,
        |assignedTo as assigned_to,
        |countedBy as confirm_by,
        |w.warehouse_name as warehouse_name,
        |w.warehouse_typ as warehouse_type,
        |w.warehouse_region as warehouse_region,
        |c.company_name as company_name,
        |u.name as assigned_name,
        |'' as assigned_dept,
        |'' as ctn_id,
        |w.warehouse_pvc as warehouse_state,
        |row_type,
        |lastUpdated as lastupdated,
        |'' as version,
        |lastUpdatedBy as lastupdatedby,
        |createdBy as createdby,
        |'' as samplingsts
        |from  $ccd_view a
        |left join dim_warehouse FOR SYSTEM_TIME AS OF a.proctime as w on a.warehouseCode = w.warehouse_code
        |left join dim_company FOR SYSTEM_TIME AS OF a.proctime as c on a.warehouseCode = c.warehouse_code and a.companyCode = c.company_code
        |left join dim_ttx_user FOR SYSTEM_TIME AS OF a.proctime as u on a.countedBy = u.code
        |union all
        |select
        |id,
        |code as task_id,
        |warehouseCode as warehouse_code,
        |'' as company_code,
        |taskNo as task_code,
        |operateLinkName as task_typ,
        |'' as internal_task_typ,
        |finishOrderNum as rcp_code,
        |'' as rcp_id,
        |'' as task_sts,
        |created as start_pick_time,
        |finishTime as end_pick_time,
        |'-1' as wave_id,
        |'' as nd_pick_sts,
        |'' as nd_start_pick_time,
        |'' as nd_end_pick_time,
        |'' as nd_pick_by,
        |created as created,
        |'' as itm_code,
        |'' as itm_name,
        |finishQtyUom as itm_qty,
        |'' as itm_from_qty,
        |'' as itm_to_qty,
        |'' as itm_from_loc,
        |'' as itm_to_loc,
        |'' as itm_from_zone,
        |'' as itm_to_zone,
        |'' as current_loc,
        |'' as plt_current,
        |'' as ctn_code,
        |'' as from_stk_code,
        |'' as to_stk_code,
        |'' as ctn_convert_cnt,
        |'' as plt_convert_cnt,
        |operatorNum as assigned_to,
        |operateLeader as confirm_by,
        |w.warehouse_name as warehouse_name,
        |w.warehouse_typ as warehouse_type,
        |w.warehouse_region as warehouse_region,
        |'' company_name,
        |operateLeaderName as assigned_name,
        |'' as assigned_dept,
        |'' as ctn_id,
        |w.warehouse_pvc as warehouse_state,
        |row_type,
        |lastUpdated as lastupdated,
        |version as version,
        |lastUpdatedBy as lastupdatedby,
        |createdBy as createdby,
        |'' as samplingsts
        |from $ovs_view a
        |left join dim_warehouse FOR SYSTEM_TIME AS OF a.proctime as w on a.warehouseCode = w.warehouse_code
        |union all
        |select
        |id,
        |'' as task_id,
        |warehouseCode as warehouse_code,
        |'' as company_code,
        |reportCode as task_code,
        |_UTF16'打印' as task_typ,
        |'' as internal_task_typ,
        |'' as rcp_code,
        |'' as rcp_id,
        |'' as task_sts,
        |printedAt as start_pick_time,
        |printedAt as end_pick_time,
        |COALESCE(reportId,'-1') as wave_id,
        |'' as nd_pick_sts,
        |'' as nd_start_pick_time,
        |'' as nd_end_pick_time,
        |'' as nd_pick_by,
        |printedAt as created,
        |'' as itm_code,
        |'' as itm_name,
        |'' as itm_qty,
        |'' as itm_from_qty,
        |'' as itm_to_qty,
        |'' as itm_from_loc,
        |'' as itm_to_loc,
        |'' as itm_from_zone,
        |'' as itm_to_zone,
        |'' as current_loc,
        |'' as plt_current,
        |'' as ctn_code,
        |'' as from_stk_code,
        |'' as to_stk_code,
        |'' as ctn_convert_cnt,
        |'' as plt_convert_cnt,
        |'' as assigned_to,
        |printedBy as confirm_by,
        |w.warehouse_name as warehouse_name,
        |w.warehouse_typ as warehouse_type,
        |w.warehouse_region as warehouse_region,
        |'' company_name,
        |u.name as assigned_name,
        |'' as assigned_dept,
        |'' as ctn_id,
        |w.warehouse_pvc as warehouse_state,
        |row_type,
        |'' as lastupdated,
        |'' as version,
        |'' as lastupdatedby,
        |printedBy as createdby,
        |'' as samplingsts
        |from $ph_view a
        |left join dim_warehouse FOR SYSTEM_TIME AS OF a.proctime as w on a.warehouseCode = w.warehouse_code
        |left join dim_ttx_user FOR SYSTEM_TIME AS OF a.proctime as u on a.printedBy = u.code
      """.stripMargin)
    tEnv.createTemporaryView("task_view",task_view)

    val taskStream: DataStream[(Boolean, TaskItemResult)] = tEnv.toRetractStream(task_view)
    val resultStream = taskStream.filter(_._1 == true).map(_._2).map(line => {
      val keyj = new JSONObject()
      val dataj = new JSONObject()
      dataj.put("id", line.id)
      dataj.put("task_id", line.task_id)
      dataj.put("warehouse_code", line.warehouse_code)
      dataj.put("company_code", line.company_code)
      dataj.put("task_code", line.task_code)
      dataj.put("task_typ", line.task_typ)
      dataj.put("internal_task_typ", line.internal_task_typ)
      dataj.put("rcp_code", line.rcp_code)
      dataj.put("rcp_id", line.rcp_id)
      dataj.put("task_sts", line.task_sts)
      dataj.put("start_pick_time", line.start_pick_time)
      dataj.put("end_pick_time", line.end_pick_time)
      dataj.put("wave_id", line.wave_id)
      dataj.put("nd_pick_sts", line.nd_pick_sts)
      dataj.put("nd_start_pick_time", line.nd_start_pick_time)
      dataj.put("nd_end_pick_time", line.nd_end_pick_time)
      dataj.put("nd_pick_by", line.nd_pick_by)
      dataj.put("task_cre_time", line.task_cre_time)
      dataj.put("itm_code", line.itm_code)
      dataj.put("itm_name", line.itm_name)
      dataj.put("itm_qty", line.itm_qty)
      dataj.put("itm_from_qty", line.itm_from_qty)
      dataj.put("itm_to_qty", line.itm_to_qty)
      dataj.put("itm_from_loc", line.itm_from_loc)
      dataj.put("itm_to_loc", line.itm_to_loc)
      dataj.put("itm_from_zone", line.itm_from_zone)
      dataj.put("itm_to_zone", line.itm_to_zone)
      dataj.put("current_loc", line.current_loc)
      dataj.put("plt_current", line.plt_current)
      dataj.put("ctn_code", line.ctn_code)
      dataj.put("from_stk_code", line.from_stk_code)
      dataj.put("to_stk_code", line.to_stk_code)
      dataj.put("ctn_convert_cnt", line.ctn_convert_cnt)
      dataj.put("plt_convert_cnt", line.plt_convert_cnt)
      dataj.put("assigned_to", line.assigned_to)
      dataj.put("confirm_by", line.confirm_by)
      dataj.put("warehouse_name", line.warehouse_name)
      dataj.put("warehouse_type", line.warehouse_type)
      dataj.put("warehouse_region", line.warehouse_region)
      dataj.put("company_name", line.company_name)
      dataj.put("assigned_name", line.assigned_name)
      dataj.put("assigned_dept", line.assigned_dept)
      dataj.put("ctn_id", line.ctn_id)
      dataj.put("warehouse_state", line.warehouse_state)
      dataj.put("row_type", line.row_type)
      dataj.put("lastupdate", line.lastupdate)
      dataj.put("version", line.version)
      dataj.put("lastupdatedby", line.lastupdatedby)
      dataj.put("createdby", line.createdby)
      dataj.put("samplingsts", line.samplingsts)
      keyj.put("key", line.id + line.warehouse_code + line.company_code + line.task_typ)
      keyj.put("data", dataj)
      keyj.toString
    })
    val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapServers)
    val kafkaSink = new FlinkKafkaProducer011[String](sinkTopic, new KafkaKeySchema(), props,
      Optional.ofNullable(new KafkaPartitionSchema),FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE, 15)
    resultStream.addSink(kafkaSink)


    env.execute("DwdWmsTaskItm")
  }

}
