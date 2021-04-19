# -*- coding: utf-8 -*-
# author: lj
# input: tmp.dim_ad_push_device_info_df
# output: tmp.dwd_ad_push_device_info_df
# 基本逻辑：文本中提取重要数据，数据格式统一化，统一数据单位，用不到的字段暂时置空。
#
import os
import re
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
import sys,traceback,datetime
from pyspark.sql.types import *

#from pyspark import StorageLevel

def is_empty(arg):
    return True if arg is None else not bool(re.search('\S', arg))

nvl3 = lambda a, b: b if is_empty(a) else a

def get_phone_size_l(phone_size):
    match=re.findall('(\d+\.*\d*)',str(phone_size))
    length= len(match)
    if length>=1:
        size_l=max([float(x) for x in match])
        if int(size_l)>50 & int(size_l)<250:
            return size_l
    return None

def get_phone_size_w(phone_size):
    match=re.findall('(\d+\.*\d*)',str(phone_size))
    length= len(match)
    if length >=2:
        size_w=sorted([float(x) for x in match])[-2]
        if int(size_w)>10 & int(size_w)<150:
            return size_w
    return None

def get_phone_size_h(phone_size):
    match=re.findall('(\d+\.*\d*)',str(phone_size))
    length= len(match)
    if length >=3:
        size_h=sorted([float(x) for x in match])[-3]
        if int(size_h>1) & int(size_h<20):
            return size_h
    return None


def get_sc_res_l(sc_resolution):
    match=re.search('(\d+)[X*×](\d+)',str(sc_resolution))
    if match:
        ma=max(int(match.group(1)),int(match.group(2)))
        if ma>100 & ma<10000:
            return ma
    return None

def get_sc_res_w(sc_resolution):
    match=re.search('(\d+)[X*×](\d+)',str(sc_resolution))
    if match:
        mi=min(int(match.group(1)),int(match.group(2)))
        if mi>100 & mi<10000:
            return mi
    return None

def resolve_cpu_ghz(cpu_hz_raw):
    if not cpu_hz_raw:
        return None
    pn = r'\d+(?:\.\d+)?'
    ghz = [float(x.group(1)) / 1000 for x in re.finditer(r'(%s)\s*(?:mhz)' % pn, str(cpu_hz_raw).lower())]
    ghz += [float(x.group(1)) for x in re.finditer(r'(%s)\s*(?:ghz)' % pn, str(cpu_hz_raw).lower())]
    return round(sum(ghz) / len(ghz),2) if len(ghz) > 0 else None

def resolve_camera_resolution_m(resolution_raw):
    if not resolution_raw:
        return None
    else:
        list=[int(x) for x in re.findall('(\d+\.*\d*)万',str(resolution_raw))]
        list+=[int(float(x)*10000) for x in re.findall('(\d+\.*\d*)亿',str(resolution_raw))]
        if list:
            return max(list)
        return None

def resolve_camera_resolution_s(resolution_raw):
    if not resolution_raw:
        return None
    else:
        list=[int(x) for x in re.findall('(\d+\.*\d*)万',str(resolution_raw))]
        list+=[int(float(x)*10000) for x in re.findall('(\d+\.*\d*)亿',str(resolution_raw))]
        if len(list)>=2:
            return sorted(list)[-2]
        return None

def get_charge_mode(charge_mode):
    l1=[float(x) for x in re.findall('(\d+\.*\d*)W',str(charge_mode))]
    res=re.findall('(\d+\.*\d*)V/(\d+\.*\d*)A', str(charge_mode), re.I)
    l1+=[float(x) * float(y) for (x, y) in res]
    if l1:
        return max(l1)
    return None

def get_ram_rom(ram):
    list = [float(x) for x in re.findall('(\d+\.*\d*)G',str(ram),re.I)]
    list += [float(x)/1024 for x in re.findall('(\d+\.*\d*)M', str(ram), re.I)]
    if list:
        return sum(list) / len(list)
    return None


def process(spark,pt_d,pt_drop):
    sql1='''
    CREATE EXTERNAL TABLE IF NOT EXISTS tmp.dwd_ad_push_device_info_df(
         imei             string       COMMENT'IMEI,platform="A"的imei'
        ,model            string       COMMENT'上报机型'
        ,model_name       string       COMMENT'设备市场名称'
        ,brand            string       COMMENT'设备品牌'
        ,sub_brand        string       COMMENT'设备子品牌'
        ,exp_year         int          COMMENT'上市年份，例：2020'
        ,price            int          COMMENT'价格，单位：元'
        ,sim_slot         int          COMMENT'卡槽个数'
        ,os_version       string       COMMENT'操作系统'
        ,time_zone        int          COMMENT'时区'
        ,language         string       COMMENT'语言'
        ,dev_type         string       COMMENT'设备形式：手机、计算机，智能手表，IOT设备等'
        ,phone_size_l     int          COMMENT'手机长 单位：mm'
        ,phone_size_w     int          COMMENT'手机宽 单位：mm'
        ,phone_size_h     int          COMMENT'手机厚 单位：mm'
        ,phone_weight     int          COMMENT'手机重量'
        ,sc_res_l         int          COMMENT'屏幕分辨率长'
        ,sc_res_w         int          COMMENT'屏幕分辨率宽'
        ,sc_size          double       COMMENT'屏幕尺寸'
        ,sc_type          string       COMMENT'屏幕类型'
        ,battery          int          COMMENT'电池容量 单位：mAh'
        ,cpu_hz           double       COMMENT'CPU频率,ghz'
        ,cpu_model        string       COMMENT'CPU型号'
        ,cpu_cnt          int          COMMENT'CPU核心数'
        ,bk_resolution_m  int          COMMENT'后摄分辨率,单位：万像素'
        ,bk_resolution_s  int          COMMENT'后摄分辨率,单位：万像素'
        ,fr_resolution_m  int          COMMENT'前摄分辨率,单位：万像素'
        ,fr_resolution_s  int          COMMENT'前摄分辨率,单位：万像素'
        ,nfc              string       COMMENT'nfc功能 是：1，否0，NULL'
        ,sensor           string       COMMENT'感应器类型'
        ,unlock_mode      string       COMMENT'解锁方式'
        ,charge_mode      int          COMMENT'充电功率： 单位W'
        ,network          string       COMMENT'支持的网络制式'
        ,ram              int          COMMENT'运行内存,单位GB'
        ,rom              int          COMMENT'存储空间,单位GB'
        ,source           string       COMMENT'sdk,gsma,crawl,model_crawl'
        ,dw_create_time   string       COMMENT'数据生成时间'
    )                                  COMMENT '设备维度信息表(platform="A")'
    PARTITIONED by (pt_d  BIGINT       COMMENT'分区字段(yyyymmdd)')
    STORED AS parquet TBLPROPERTIES ('parquet.compression'='snappy')
    Location 'hdfs://nameservice1/user/hive/warehouse/tmp.db/dwd_ad_push_device_info_df'
    '''
    print(sql1)
    spark.sql(sql1)

    sql2=f'''
    insert overwrite table tmp.dwd_ad_push_device_info_df partition (pt_d={pt_d})
    select
         nvl3(imei      ,NULL)
        ,nvl3(model     ,NULL)
        ,nvl3(model_name,NULL)
        ,nvl3(brand     ,NULL)
        ,nvl3(sub_brand ,NULL)
        ,if(exposure_date<=year(now()),exposure_date,NULL)
        ,if(price>0,price,NULL)
        ,if(sim_slot>=0 and sim_slot<=4,sim_slot,NULL)
        ,nvl3(os_version,NULL)
        ,if(time_zone>=-12 and time_zone<=12 ,time_zone,NULL)
        ,nvl3(language,NULL)
        ,nvl3(dev_type,NULL)
        ,get_phone_size_l(phone_size)
        ,get_phone_size_w(phone_size)
        ,get_phone_size_h(phone_size)
        ,nvl3(phone_weight,NULL)
        ,get_sc_res_l(sc_resolution)
        ,get_sc_res_w(sc_resolution)
        ,nvl3(sc_size,NULL)
        ,nvl3(sc_type,NULL)
        ,nvl3(battery,NULL)
        ,resolve_cpu_ghz(cpu_hz)
        ,case
            when upper(cpu_model) rlike '海思|麒麟|HUAWEI'  then '海思'
            when cpu_model rlike '高通|骁龙'                then '高通'
            when instr(cpu_model, '联发科') > 0             then '联发科'
            when instr(cpu_model, '三星') > 0               then '三星'
            when instr(cpu_model, '苹果') > 0               then '苹果'
            when instr(upper(cpu_model), 'INTEL') > 0       then 'INTEL'
            when instr(upper(cpu_model), 'ARM') > 0         then 'ARM'
            when instr(upper(cpu_model), 'AMD') > 0         then 'AMD'
            when instr(upper(cpu_model), 'AARCH') > 0       then 'AARCH'
            when instr(upper(cpu_model), 'MARVELL') > 0     then 'MARVELL'
                                                            else NULL  end  as cpu_model
        ,case
            when cpu_cnt rlike '十六'     then 16
            when cpu_cnt rlike '十二'     then 12
            when cpu_cnt rlike '十'       then 10
            when cpu_cnt rlike '八|双四'  then 8
            when cpu_cnt rlike '六'       then 6
            when cpu_cnt rlike '五'       then 5
            when cpu_cnt rlike '四'       then 4
            when cpu_cnt rlike '三'       then 3
            when cpu_cnt rlike '双'       then 2
            when cpu_cnt rlike '单'       then 1
                                          else NULL  end                as  cpu_cnt
        ,resolve_camera_resolution_m(bk_resolution)
        ,resolve_camera_resolution_s(bk_resolution)
        ,resolve_camera_resolution_m(fr_resolution)
        ,resolve_camera_resolution_s(fr_resolution)
        ,case
            when nfc rlike '支持|是' then 1
            when nfc rlike '否'      then 0
                                     else NULL     end                  as  nfc
        ,NULL                                                           as  sensor
        ,NULL                                                           as  unlock_mode
        ,get_charge_mode(charge_mode)                                   as  charge_mode
        ,NULL                                                           as  network
        ,get_ram_rom(ram)                                               as  ram
        ,get_ram_rom(rom)                                               as  rom
        ,nvl3(source,NULL)
        ,current_timestamp()
    from tmp.dim_ad_push_device_info_df
    where pt_d={pt_d}
    '''
    print(sql2)
    spark.sql(sql2)

    sql3=f'''
    alter table tmp.dwd_ad_push_device_info_df drop if exists partition(pt_d={pt_drop})
    '''
    print(sql3)
    spark.sql(sql3)



def createSpark(appName):
    conf = SparkConf().setAppName(appName)
    conf.set("spark.yarn.queue", "root.bpa.ad_push.kdd")
    #conf.set("spark.rdd.compress", "true")
    #conf.set("spark.broadcast.compress", "true")
    conf.set("spark.executor.instances", '200')
    conf.set("spark.executor.cores", '2') #spark.executor.cores：顾名思义这个参数是用来指定executor的分配更多的内核意味着executor并发能力越强，能够同时执行更多的task
    conf.set('spark.executor.memory', '32g')  #executor memory是每个节点上占用的内存。每一个节点可使用内存
    #conf.set('spark.default.parallelism','4000')
    #conf.set('spark.executor.memoryOverhead','10g')
    #conf.set('spark.dynamicAllocation.initialExecutors','200')
    #conf.set('spark.dynamicAllocation.minExecutors','100')
    #conf.set('spark.dynamicAllocation.maxExecutors','500')
    #conf.set("spark.sql.shuffle.partitions", "500") # 设置shuffle分区数
    #conf.set("spark.driver.maxResultSize", "5g")
    conf.set("spark.sql.hive.mergeFiles", "true")
    conf.set("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.6")
    conf.set("spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON", "/usr/local/bin/python3.6")
    conf.set("spark.executorEnv.PYTHONPATH",
             "/usr/local/anaconda3/bin/python3.6/site-packages:/opt/spark-2.4.3-bin-hadoop2.6/python:/opt/spark-2.4.3-bin-hadoop2.6/python/lib/py4j-0.10.7-src.zip")
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    return spark


if __name__ == "__main__":
    os.environ['PYSPARK_PYTHON'] = "/usr/local/bin/python3.6"  # 该设置仅在当前程序有效 ,python3.6方式启动时才需
    pt_d=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime("%Y%m%d")
    pt_drop=(datetime.datetime.now()+datetime.timedelta(days=-5)).strftime("%Y%m%d")
    pt_h=None
    l=len(sys.argv)
    if l>=2:
        pt_d = sys.argv[1]
        if l>=3:
            pt_h = sys.argv[2]
    appName = "dwd_ad_push_device_info_df"
    spark = createSpark(appName=appName)
    spark.udf.register('get_phone_size_l', get_phone_size_l,StringType())
    spark.udf.register('get_phone_size_w', get_phone_size_w,StringType())
    spark.udf.register('get_phone_size_h', get_phone_size_h,StringType())
    spark.udf.register('get_sc_res_l', get_sc_res_l,StringType())
    spark.udf.register('get_sc_res_w', get_sc_res_w,StringType())
    spark.udf.register('resolve_cpu_ghz', resolve_cpu_ghz,StringType())
    spark.udf.register('resolve_camera_resolution_m', resolve_camera_resolution_m,StringType())
    spark.udf.register('resolve_camera_resolution_s', resolve_camera_resolution_s,StringType())
    spark.udf.register('get_charge_mode', get_charge_mode,StringType())
    spark.udf.register('get_ram_rom', get_ram_rom,StringType())
    spark.udf.register('is_empty', is_empty,BooleanType())
    spark.udf.register('nvl3', nvl3,StringType())
    print("pt_d: ",pt_d,"       pt_h: ",pt_h)
    try:
        process(spark,pt_d,pt_drop)
        print("████████ mission completed ████████")
    except Exception as ex:
        traceback.print_exc()
        print("████████ something is wrong ████████")
    finally:
        spark.stop()
