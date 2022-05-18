#!/usr/bin/python27
#coding=utf-8
import pymysql
import cx_Oracle
import sys
import logging
import logging.config
import HiveMetaDao
import os
from HiveTask import *
#from CommonLog import logger
from ExecLogging import MyLogging
reload(sys)
sys.setdefaultencoding('utf-8')
ddl_base_dir="/data/syxf/zjj/dw_create/ods/"
ods_base_dir="/data/ods"
mylog = MyLogging(task_name=sys.argv[6] )
mylog.set_level("CONSOLE")
logger = mylog.get_logger()
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK'
'''初始化数据库连接信息'''
def init_db(ip,db_name,db_user,db_pass):
	logger.info("###############被同步的数据库连接信息###################")
	logger.info("ip="+ip)
	logger.info("db_name="+db_name)
	logger.info("db_user="+db_user)
	try:
		conn = cx_Oracle.connect("%s/%s@%s/%s"%(db_user,db_pass,ip,db_name))
		
		return conn
	except cx_Oracle.DatabaseError as exc:
                error, = exc.args
		logger.error("Oracle Error %d: %s" % (error.code, error.message))
		raise error.context

'''
查询oracle的meta信息，根据字典名称、类型、注释生成hive表结构
'''		
def constructHiveOdsLoad(conn,tb,hive_db,hive_tb,src_db,create_type,is_partition,key,begin_date,stg_table,load_type):
	colum_map={}
	try:
                cursor=conn.cursor()
		columnCommentInfo(colum_map,cursor,tb,src_db)
                tb_db=src_db+"."+tb
                colums = ''
                colums_a = ''
                ods_table = hive_tb
                for com in colum_map:
                    field=colum_map[com]	       
                    colums=colums+"  "+field[1].lower()+" ,\n"
                    colums_a=colums_a+"  a."+field[1].lower()+" ,\n"
		if len(colums)>0:
			colums=colums[:-2]
                if len(colums_a)>0:
			colums_a=colums_a[:-2]
                sb='''#/bin/bash
source /etc/profile
#--------------------------------------------------------
# The input arguments, that is determined by user.
#--------------------------------------------------------
if [ $# -eq 0 ];then
    DAY=`date --date '-1 day' +%Y-%m-%d`
elif [ $# -eq 1 ];then
    DAY=$1
elif [ $# -eq 2 ];then
    BEGDAY=$1
    ENDDAY=$2
else
     echo 'please input args ,example : [sh xx.sh 2017-07-01] or [sh xx.sh] or [sh xx.sh 2017-07-01 2017-07-31]';exit 1;
fi



echo "DAY=$DAY"
echo "$1"

function calc_product()
{
#---------------------------------------------------------
# The SQL variables, that could be changed by user.
#---------------------------------------------------------

#===============================================================
#desc    : '''
                sb=sb+ods_table
                sb=sb+'''_ETL操作
#mode    : T-1
#target  : ods.'''+ods_table+''' 
#source  : stg.'''+stg_table+'''
#version :  v1.0
#date          modifier         desc
#===============================================================
#2020-02-26      zjj             
#=============================================================== 
 SQL=" 
'''
                if load_type=='1':
                   if is_partition=="Y":
                       sb=sb+'''insert overwrite table ods.%s partition (bdw_statis_date='${DAY}')
        select  CURRENT_TIMESTAMP as bdw_insert_time,
                '${DAY}'  as bdw_etl_date,
                %s
        from stg.%s
        where etl_date='${DAY}'
'''%(ods_table,colums,stg_table) 
                   elif is_partition=="N":
                       sb=sb+'''insert overwrite table ods.%s 
        select cast(CURRENT_TIMESTAMP as string) as bdw_insert_time,
                '${DAY}'  as bdw_etl_date,
                %s
        from stg.%s
        where etl_date='${DAY}'
        union all 
        select  bdw_insert_time,
                bdw_etl_date,
                %s
        from ods.%s
'''%(ods_table,colums,stg_table,colums,ods_table) 
                elif load_type=='2':
                   key_arr=key.split(';')
                   key_join=""
                   key_where=""
                   key_colums=""
                   for key_col in  key_arr:
                       key_join=key_join+" a."+key_col+"=c."+key_col+" and"
                       key_where="c."+key_col+" is null"
                       key_colums=key_colums+key_col+" ,"

                   if len(key_join)>0:
			    key_join=key_join[:-4]
                   if len(key_colums)>0:
	                    key_colums=key_colums[:-2]

                   if is_partition=="Y":

                       sb=sb+'''set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert  overwrite table ods.%s  partition (bdw_statis_date) 
        select   cast(CURRENT_TIMESTAMP as string) as bdw_insert_time,
                 '${DAY}' bdw_etl_date,
                 %s,
                 %s as bdw_statis_date
        from stg.%s
        where etl_date='${DAY}'        
        union all
        select bdw_insert_time,
               bdw_etl_date,
               %s,
               a.%s as bdw_statis_date              
        from    ods.%s a
        inner join (select %s from stg.%s where etl_date='${DAY}' 
                    group by %s) b 
        on a.%s=b.%s
        left join (select %s from stg.%s where etl_date='${DAY}')c
        on %s
        where %s
'''%(ods_table,colums,begin_date,stg_table,colums_a,begin_date,ods_table,begin_date,stg_table,begin_date,begin_date,begin_date,key_colums,stg_table,key_join,key_where) 

                   elif is_partition=="N":
                       sb=sb+'''        
        insert  overwrite table ods.%s
        select   cast(CURRENT_TIMESTAMP as string) as bdw_insert_time,
                 '${DAY}' bdw_etl_date,
                 %s
        from stg.%s
        where etl_date='${DAY}'        
        union all
        select bdw_insert_time,
               bdw_etl_date,
               %s              
        from    ods.%s a
        left join (select %s from stg.%s where etl_date='${DAY}')c
        on %s
        where %s
'''%(ods_table,colums,stg_table,colums_a,ods_table,key_colums,stg_table,key_join,key_where) 

                elif load_type=='3':
                   key_colums=""
                   key_arr=key.split(';')
                   for key_col in  key_arr:
                        key_colums=key_colums+key_col+" ,"
                   if len(key_colums)>0:
                        key_colums=key_colums[:-2]
                   if is_partition=="Y":
                       sb=sb+''' set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert  overwrite table ods.%s  partition (bdw_statis_date) 
        select CURRENT_TIMESTAMP as bdw_insert_time,
               bdw_etl_date,
	       bdw_start_date,
               if(nu=1,'2999-12-31','${DAY}') bdw_end_date,
               %s,
               if(nu=1,'2999-12-31','${DAY}') bdw_statis_date
        from (select bdw_start_date,bdw_etl_date,%s
                    ,row_number()over(partition by %s order by %s desc) nu
              from (select '${DAY}' bdw_start_date
                           ,'${DAY}' bdw_etl_date
                           ,'2999-12-31' bdw_end_date
                           ,%s
                    from stg.%s
                    where etl_date='${DAY}'
                    union all 
                    select bdw_start_date,bdw_etl_date,bdw_end_date
                           ,%s
                    from ods.%s 
                    where bdw_statis_date='2999-12-31'
              )t
        )ta
'''%(ods_table,colums,colums,key_colums,begin_date,colums,stg_table,colums,ods_table) 
                   elif is_partition=="N":
                       sb=sb+''' insert  overwrite table ods.%s  
        select CURRENT_TIMESTAMP as bdw_insert_time,
               bdw_etl_date,
	           bdw_start_date,
               if(nu=1,'2999-12-31','${DAY}') bdw_end_date,
               %s 
        from (select bdw_start_date,bdw_etl_date,%s
                    ,row_number()over(partition by %s order by %s desc) nu
              from (select '${DAY}' bdw_start_date
                           ,'${DAY}' bdw_etl_date
                           ,'2999-12-31' bdw_end_date
                           ,%s
                    from stg.%s
                    where etl_date='${DAY}'
                    union all 
                    select bdw_start_date,bdw_etl_date,bdw_end_date
                           ,%s
                    from ods.%s 
                    where bdw_end_date='2999-12-31'
              )t
        )ta
        union all 
        select CURRENT_TIMESTAMP bdw_insert_time,bdw_etl_date,bdw_start_date
         ,bdw_end_date
         ,%s
        from ods.%s
        where bdw_end_date<'${DAY}'
'''%(ods_table,colums,colums,key_colums,begin_date,colums,stg_table,colums,ods_table,colums,ods_table) 

                elif load_type=='4':
                   if is_partition=="Y":
                       sb=sb+''' set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert overwrite table ods.%s partition(bdw_statis_date)
        select CURRENT_TIMESTAMP as bdw_insert_time,
               '${DAY}' bdw_etl_date,
               %s,
               to_date(%s) bdw_statis_date
        from     stg.%s    
        where etl_date='${DAY}'
'''%(ods_table,colums,begin_date,stg_table) 
                   elif is_partition=="N":
                       sb=sb+''' insert overwrite table ods.%s 
        select CURRENT_TIMESTAMP as bdw_insert_time,
               '${DAY}' bdw_etl_date,
               %s
        from     stg.%s    
        where etl_date='${DAY}'
'''%(ods_table,colums,stg_table)

                elif load_type=='5':
                   key_colums=""
                   key_arr=key.split(';')
                   for key_col in  key_arr:
                        key_colums=key_colums+key_col+" ,"
                   if len(key_colums)>0:
     	                key_colums=key_colums[:-2]
                   if is_partition=="Y":
                       sb=sb+''' set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert  overwrite table ods.%s  partition (bdw_statis_date) 
        select CURRENT_TIMESTAMP as bdw_insert_time,
               bdw_start_date bdw_etl_date,
               bdw_start_date,
               if(sn=1,'2999-12-31','${DAY}') bdw_end_date,
               %s,
               if(sn=1,'2999-12-31','${DAY}') bdw_statis_date
        from (
            select bdw_start_date,%s
                  ,row_number()over(partition by %s order by %s desc) sn 
            from (
                select %s,min(bdw_start_date) bdw_start_date
                from (
                    select %s,'${DAY}' bdw_start_date
                    FROM stg.%s 
                    where etl_date='${DAY}'
                    union all 
                    select %s,bdw_start_date
                    from ods.%s 
                    where bdw_statis_date='2999-12-31'
                )ta  
                group by %s
            )tb        
        )tc               
'''%(ods_table,colums,colums,key_colums, begin_date,colums,colums,stg_table,colums,ods_table,colums) 
                   elif is_partition=="N":
                       sb=sb+''' set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        insert  overwrite table ods.%s 
        select CURRENT_TIMESTAMP as bdw_insert_time,
               bdw_start_date bdw_etl_date,
               bdw_start_date,
               if(sn=1,'2999-12-31','${DAY}') bdw_end_date,
               %s
        from (
            select bdw_start_date,%s
                  ,row_number()over(partition by %s order by %s desc) sn 
            from (
                select %s,min(bdw_start_date) bdw_start_date
                from (
                    select %s,'${DAY}' bdw_start_date
                    FROM stg.%s 
                    where etl_date='${DAY}'
                    union all 
                    select %s,bdw_start_date
                    from ods.%s 
                    where bdw_end_date='2999-12-31'
                )ta  
                group by %s
            )tb        
        )tc  
        union all  
        select %s,bdw_start_date
         from ods.%s 
        where bdw_end_date<'${DAY}'
'''%(ods_table,colums,colums,key_colums, begin_date,colums,colums,stg_table,colums,ods_table,colums,colums,ods_table) 	

                else:
                   sb="else"
	
		sb=sb+''' "
DEBUG="false"
[ "$DEBUG" == "true" ] && echo "hive -e \"$SQL\""
if [ "$DEBUG" != "true" ]; then
    sudo -uhive hive -e "$SQL"  

    if [ $? -ne 0 ]; then
      echo "[$(date "+%Y-%m-%d %H:%M:%S")] : hive run failed." 
      exit 1
    fi
fi
}


if [ $# -eq 2 ];then
    DAY=$BEGDAY
    echo $DAY
    while((`date -d "$DAY" +%Y%m%d`<`date -d "$ENDDAY" +%Y%m%d`))
        do
            calc_product $DAY
            DAY=`date -d "$DAY +1 day " +%Y-%m-%d`
            echo $DAY
        done
else
    calc_product $DAY
fi
		'''
		logger.info(sb) 
                create_table_path=ddl_base_dir+"exec-hive_"+hive_tb
                isExists=os.path.exists(create_table_path)
                if not isExists:
                   os.makedirs(create_table_path) 
                output = open(create_table_path+"/deal.sh", 'w')
		output.write("\n")
		output.write(sb)
		output.close()
                job_path=ddl_base_dir+"job_bak"
                isExists=os.path.exists(job_path)
               # print "aaa"
                if not isExists:
                   os.makedirs(job_path)
                output = open(job_path+"/exec-hive_"+hive_tb+".job", 'w')
                job_file=''' type=command
command=sh ../shell/exec-hive_%s/main.sh  ${is_createTable} ${is_init} ${bdate} ${edate}
dependencies =dw-start
'''%(ods_table)
                output.write("\n")
                output.write(job_file)
                output.close()  
		cursor.close()
		conn.close()
		return sb
	except Exception as e:
		logger.error("Error %d: %s" % (e.args[0], e.args[1]))
		raise e
'''
获取字段类型、注释信息
'''
def columnCommentInfo(mp,cursor,tb,src_db):
	try:
		sql="select column_id,column_name  FROM all_tab_columns where  OWNER='%s' AND table_name='%s'"%(src_db,tb);
                cursor.execute(sql)
		rows = cursor.fetchall()	
		for row in rows:
			mp[row[0]]=row
	except Exception as e:
		logger.error("Error %d: %s" % (e.args[0], e.args[1]))
		raise e

def main():
	try:
		ip="192.168.18.2"
		port=1521
		db_name="testdb1"
		db_user="BI_USER"
		db_pass="BI_USER"
		table="loanacctinfo"
		columns="acctnoseq,userseq,createdate,acctname"
		is_partition="true"
		dt='2020-02-24'
		hive_db="stg"
		hive_tb=db_name+"_"+table;
		if len(sys.argv)>6 :
			ip=sys.argv[1]	
			port= sys.argv[2]
			db_name=sys.argv[3]
			db_user=sys.argv[4]
			db_pass=sys.argv[5]
			table=sys.argv[6].upper()
			create_type=sys.argv[9]
			load_type=sys.argv[7]
			src_db=sys.argv[8].upper()
                        is_partition=sys.argv[10]
                        key=sys.argv[11]
                        begin_date=sys.argv[12]
                        stg_table=sys.argv[13]
		print "ip=%s,port=%s,db_name=%s,db_user=%s,db_pass=%s,table=%s,create_type=%s,load_type=%s,src_db=%s,is_partition=%s,key=%s,begin_date=%s,stg_table=%s" %(ip,str(port),db_name,db_user,db_pass,table,create_type,load_type,src_db,is_partition,key,begin_date,stg_table)
		conn=init_db(ip=ip,db_name=db_name,db_user=db_user,db_pass=db_pass)
		print sys.argv[1]
		sys.argv[1]="sync"
		print sys.argv[1]
                hive_tb=src_db.lower()+"_"+table.lower()+"_"+create_type
                hive_db='ods'

		table_ddl=constructHiveOdsLoad(conn=conn,tb=table,hive_db=hive_db,hive_tb=hive_tb,src_db=src_db,create_type=create_type,is_partition=is_partition,key=key,begin_date=begin_date,stg_table=stg_table,load_type=load_type)		
	except Exception as e:
		logger.error("Error  %s" % (e.args[0]))
		logger.error("创建表%s或分区%s失败,具体见日志!" %(hive_tb,dt))
		raise e	

if __name__ == '__main__':
    main()











#insert overwrite table ods.ods_vsomsdt_s_t_cuufaf_primary_order_orgcode
#select id,org_code,org_code_name,is_delete,is_delete_name,remark,create_time,update_time,current_timestamp() dw_entry_time
#from 
#(select id,org_code,org_code_name,is_delete,is_delete_name,remark,create_time,update_time
#from ods.ods_vsomsdt_d_t_cuufaf_primary_order_orgcode
#where dt='$v_date'
#union all 
#select t1.id,org_code,org_code_name,is_delete,is_delete_name,remark,create_time,update_time
#from ods.ods_vsomsdt_s_t_cuufaf_primary_order_orgcode t1
#left join 
#     (select id
#      from ods.ods_vsomsdt_d_t_cuufaf_primary_order_orgcode
#      where dt='$v_date'
#      )t2
#on t1.id=t2.id 
#where t2.id is null
#)tt1;