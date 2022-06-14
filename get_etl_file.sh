#!/bin/bash
set -u

### update grep 
### remove rowcount

#if [ $# -ne 2 ];then
#	echo "Error: Uasge: $0 <Yaml File Name> <Stored File Full Path>"
#	echo "Example: sh get_etl_file.sh doris.yaml /c/Users/fwen/4test"
#    exit 2
#fi

if [ $# -ne 1 ];then
	echo "Error: Uasge: $0 <Yaml File Name>"
	echo "Example: sh get_etl_file.sh /c/Users/fwen/doris.yaml"
    exit 2
fi


yaml_file=$1
#store_path=$2
store_path=$(cd `dirname $0`; pwd)


mode_flag="default"
cat << EOF
----------------------------------------
|****Choose your copy mode ****|
----------------------------------------
`echo " 1 : From Local Git Repository"`
`echo " 2 : From Tunnel Port"`
EOF
read -p "input your select num：" num1
case $num1 in
 1)
  echo "your selection numberis 1, pls input your local git repository etl path, like /c/Users/fwen/DINT-CLSFD: "
  read input_repo_path
  mode_flag="repo"
  ;;
 2)
  echo "your selection number is 2, pls input your ebay production tunnel port, like 30019: "
  read input_tunnel_port
  mode_flag="tunnel"
  ;;
 *)
  echo "Error input number"
  exit 2
esac

echo "########################################################################"
echo "#******************************LOG-INFO*********************************"
echo "########################################################################"

if [ "${mode_flag}" == "repo" ];then
	echo "Info: input repository path: $input_repo_path"
elif [ "${mode_flag}" == "tunnel" ];then
	echo "Info: input tunnel port: $input_tunnel_port"
fi
echo "########################################################################"

uNames=`uname -s`
osName=${uNames: 0: 4}
echo "Info: current OS: $uNames"
echo "########################################################################"
current_user=`whoami`
work_home=${store_path}/for_${current_user}_test
if [ ! -d "${work_home}" ];then
	mkdir -p ${work_home}
	chmod 775 ${work_home}
fi

input_yaml_file=$work_home/input.yaml
cp $yaml_file $input_yaml_file

cfg_home=${work_home}/cfg
if [ ! -d "${cfg_home}" ];then
	mkdir -p ${cfg_home}
	chmod 775 ${cfg_home}
else
	rm ${cfg_home}/*.cfg
fi

seq_home=${work_home}/seq
if [ ! -d "${seq_home}" ];then
	mkdir -p ${seq_home}
	chmod 775 ${seq_home}
else
	rm ${seq_home}/*.seq
fi

sql_home=${work_home}/sql
if [ ! -d "${sql_home}" ];then
	mkdir -p ${sql_home}
	chmod 775 ${sql_home}
else
	rm ${sql_home}/*.sql
fi

cd ${work_home}
tmp_etl_file=all.etl.tmp
etl_file=all.etl
seq_file=all.seq
sql_file=all.sql
cfg_file=all.cfg
tmp_pv_file=all.pv.tmp
pv_file=all.pv
output_file=all.output
detail_file=all.detail
rm all.*


# 1. get variable and value keypair from stt etl id
cat ${input_yaml_file} |grep "clsfd_aws_single_table_transform_handler" |awk -F'clsfd_aws_single_table_transform_handler.ksh' '{print $2}'|awk '{print $1}'|awk '{print $1}'|awk '$1=$1'|grep "&"| awk -F '&' '{print $2}'|awk -F '#' '{print $1}'|uniq >> $tmp_pv_file
cat ${input_yaml_file} |grep -i -E "clsfd_multiply_update_hdfs_handler"| awk -F'clsfd_multiply_update_hdfs_handler.ksh' '{print $2}'|awk  '{print $1}'|awk '{print $1}'|awk '$1=$1'|grep "&"| awk -F '&' '{print $2}'|awk -F '#' '{print $1}'|uniq >> $tmp_pv_file 

cat ${tmp_pv_file}|uniq|while read parm 
do
	value=`cat ${input_yaml_file} |grep -E -i "${parm}*:"|awk -F":" '{print $2}'|sed "s/\"//g"|sed "s/'//g"|awk '{print $1}'|awk '$1=$1'`
	echo $parm:$value >> ${pv_file}
done


# 2. get etl id list
cat ${input_yaml_file}|grep "clsfd_aws_single_table_transform_handler" |awk -F'clsfd_aws_single_table_transform_handler.ksh' '{print $2}'|awk '{print $1}'|awk '{print $1}'|awk '$1=$1' |uniq >> ${tmp_etl_file}
cat ${input_yaml_file}|grep -i -E "clsfd_multiply_update_hdfs_handler"| awk -F'clsfd_multiply_update_hdfs_handler.ksh' '{print $2}'|awk  '{print $1}'|awk '{print $1}'|awk '$1=$1'|uniq >> ${tmp_etl_file}

cat ${tmp_etl_file}|uniq|while read line 
do
	if [[ "${line}" =~ "#" ]];then
		tmp_vara=`echo ${line#*&}`
		vara=`echo ${tmp_vara%#*}`
		#value=`cat ${pv_file} |grep -i "${vara}"|awk -F':' '{print $2}'|awk '{print $1}'|awk '$1=$1'`
		for i in `cat ${pv_file} |grep -i "${vara}"|awk -F':' '{print $2}'`;
		do
			value=`echo $i|awk '{print $1}'|awk '$1=$1'`
			etl_id=`echo ${line}|sed 's/&//g'|sed 's/#//g'|sed "s/$vara/$value/g"`  
			echo ${etl_id} >> ${etl_file}
		done
	else
		etl_id=`echo $line`
		echo ${etl_id} >> ${etl_file}
	fi
done

# 3. download seq and cfg file according to the etl id file
if [ -e ${etl_file} ];then
	cat ${etl_file}|uniq|while read line 
	do
		etl_id=`echo $line|awk '{print $1}'|awk '$1=$1'`
		if [ "${mode_flag}" == "repo" ];then
			echo ${etl_id}_stt.cfg >> ${cfg_file}
			cp ${input_repo_path}/etl/cfg/dw_clsfd/${etl_id}_stt.cfg ${cfg_home}/ ;
			echo ${etl_id}_stt.sql.seq >> ${seq_file}
			cp ${input_repo_path}/etl/sql/dw_clsfd/${etl_id}_stt.sql.seq ${seq_home}/ ;
		elif [ "${mode_flag}" == "tunnel" ];then
			echo ${etl_id}_stt.cfg >> ${cfg_file}
			scp -r -P $input_tunnel_port $current_user@127.0.0.1:/dw/etl/home/prod_clsfd/cfg/dw_clsfd/${etl_id}_stt.cfg ${cfg_home}/ ;
			echo ${etl_id}_stt.sql.seq >> ${seq_file}
			scp -r -P $input_tunnel_port $current_user@127.0.0.1:/dw/etl/home/prod_clsfd/sql/dw_clsfd/${etl_id}_stt.sql.seq ${seq_home}/ ;
		fi
	done
else
	echo "Error: no such etl file ${work_home}/${etl_file} generated"
	exit 2
fi

if [ -e ${cfg_file} -a -e ${seq_file} ];then
	echo "Info: full cfg file list: ${work_home}/${cfg_file}"
	echo "Info: download cfg file under ${cfg_home} successfully"
	echo "########################################################################"
	
	echo "Info: full seq file list: ${work_home}/${seq_file}"
	echo "Info: download seq file under ${seq_home} successfully"
	echo "########################################################################" 
else
	echo "Error: no such cfg file ${work_home}/${cfg_file} or seq file ${work_home}/${seq_file} generated"
	exit 2
fi




# 4. get sql file list
downloaded_seq_file_cnt=`ls ${seq_home}/*.seq |wc -l`
if [[ ${downloaded_seq_file_cnt} -gt 0 ]];then
	#all_seq_file_cnt=`cat ${seq_file}|sed '/^$/d'|wc -l`
	#if [[ ${downloaded_seq_file_cnt} -eq ${all_seq_file_cnt} ]];then
	for seq in `ls ${seq_home}/*.seq`
	do
		for line in `cat $seq`
		do
			echo $line >> $sql_file
		done
	done
	#fi
else
	echo "Error: downloaded seq file cnt is less than 0 means download failed"
	exit 2
fi



# 4. download sql file from ETL server to local
if [ -e ${sql_file} ];then
	echo "Info: full sql file list: ${work_home}/${sql_file}"
	cat ${sql_file}|sed '/^$/d'|while read line;
	do
		if [ "${mode_flag}" == "repo" ];then
			cp ${input_repo_path}/etl/sql/dw_clsfd/${line} ${sql_home}/ ;
		elif [ "${mode_flag}" == "tunnel" ];then 
			scp -r -P $input_tunnel_port $current_user@127.0.0.1:/dw/etl/home/prod_clsfd/sql/dw_clsfd/${line} ${sql_home}/ ;
		fi
		
	done
else
	echo "Error: no such sql file ${work_home}/${sql_file} generated"
	exit 2
fi
echo "Info: download sql file under ${sql_home} successfully"
echo "########################################################################"


# 5. proceed downloaded cfg file 
downloaded_cfg_file_cnt=`ls ${cfg_home}/*.cfg |wc -l`
if [[ ${downloaded_cfg_file_cnt} -gt 0 ]];then
	#all_cfg_file_cnt=`cat ${cfg_file}|sed '/^$/d'|wc -l`
	#if [[ ${downloaded_cfg_file_cnt} -eq ${all_cfg_file_cnt} ]];then
		if [[ "${osName}" == "MSYS" ]] || [[ "${osName}" == "Linu" ]];then
			sed -i 's/spark.yarn.queue/#spark.yarn.queue/g' ${cfg_home}/*.cfg
		else
			sed -i "" 's/spark.yarn.queue/#spark.yarn.queue/g' ${cfg_home}/*.cfg
		fi
		echo "Info: proceed stt remove queue under ${cfg_home} successfully"
		echo "########################################################################"
	#fi
else
	echo "Error: downloaded cfg file cnt is less than 0 means download failed"
	exit 2
fi





# 6. proceed downloaded sql file 
downloaded_sql_file_cnt=`ls ${sql_home}/*.sql |wc -l`
if [[ ${downloaded_sql_file_cnt} -gt 0 ]];then
	all_sql_file_cnt=`sort ${sql_file}|uniq|wc -l`
	if [[ ${downloaded_sql_file_cnt} -eq ${all_sql_file_cnt} ]];then
		
		# 1. keyword: alter table set location 
		echo "1.keyword: 'alter table set location', auto updated to 'clsfdworkingROOT':" >> $output_file
		grep -i -w "alter" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw1"}'|uniq >> $output_file
		if [[ "${osName}" == "MSYS" ]] || [[ "${osName}" == "Linu" ]];then
			sed -i 's#/sys/edw/\${zeta_env}/working/clsfd/clsfd_working/clsfd#\${clsfdworkingROOT}#g' `grep -i -w "alter" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1}'|uniq`
		else
			sed -i "" 's#/sys/edw/\${zeta_env}/working/clsfd/clsfd_working/clsfd#\${clsfdworkingROOT}#g' `grep -i -w "alter" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1}'|uniq`
		fi
		echo "########################################################################" >> ${output_file}

		# 2. keyword: clsfd.
		echo "2.keyword: 'clsfd.', auto updated 'clsfd.' to 'clsfd_':" >> $output_file
		grep  -i -E "[ ]+clsfd\." ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw2"}'|uniq >> $output_file
		if [[ "${osName}" == "MSYS" ]] || [[ "${osName}" == "Linu" ]];then
			sed -i 's/clsfd\./clsfd\_/g' `grep  -i -E "[ ]+clsfd\." ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1}'|uniq`
		else
			sed -i "" 's/clsfd\./clsfd\_/g' `grep  -i -E "[ ]+clsfd\." ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1}'|uniq`
		fi
		echo "########################################################################" >> ${output_file}


		# 3. keyword: DW_CURRENCIES|DW_DAILY_EXCHANGE_RATES|DW_CALENDAR|DW_CAL_DT
		echo "3.keyword: 'DW_CURRENCIES|DW_DAILY_EXCHANGE_RATES|DW_CALENDAR|DW_CAL_DT', need update 'clsfd_working' to 'clsfd_tables':" >> $output_file
		grep -i -E  "DW_CURRENCIES|DW_DAILY_EXCHANGE_RATES|DW_CALENDAR|DW_CAL_DT" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw3"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}


		# 4. keyword: E1 scientific notatio
		echo "4.keyword: scientific notatio 'E1X', need update by case:" >> $output_file
		grep -i "E1" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw4"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}

		# 5. keyword: from_utc_timestamp/to_utc_timestamp/from_unixtime
		echo "5.keyword: 'from_utc_timestamp' or 'to_utc_timestamp' or 'from_unixtime', need update by case:" >> $output_file
		grep -i -E "from_utc_timestamp|to_utc_timestamp|from_unixtime" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw5"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}

		# 6. keyword: create view
		echo "6.keyword: 'create view', need update to 'create or replace temporary view':" >> $output_file
		grep -i "create view" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw6"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}


		# 7. keyword: yyyy-MM-dd-HH.mm.ss
		echo "7.keyword: 'yyyy-MM-dd-HH.mm.ss', need update by case:" >> $output_file
		grep -i "yyyy-MM-dd-HH.mm.ss" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw7"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}


		# 8. keyword: is_user
		echo "8.keyword: 'is_user', need check the parameter and value in yaml:" >> $output_file
		grep -i "is_user" ${sql_home}/*.sql|grep -v "#"|awk -F ':' '{print $1,": kw8"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}

		# 9. keyword: add jar|add file
		echo "9.keyword: 'add jar' or 'add file', need update by case:" >> $output_file
		grep -i -E "add jar|add file" ${sql_home}/*.sql|grep -v "#" | awk -F ':' '{print $1,": kw9"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}

		# 10. keyword: lateral view
		echo "10.keyword: 'lateral view' along with 'create view', need update by case:" >> $output_file
		grep -i "lateral view" ${sql_home}/*.sql| awk -F ':' '{print $1,": kw10"}'|uniq >> $output_file
		echo "########################################################################" >> ${output_file}
		
		#echo "Info: proceed generate sql list which need to update,check at ${work_home}/${output_file}" 
		
		
		echo "########################################################################" >> $detail_file
		echo "#****************************KEYWORD-LIST******************************"  >> $detail_file
		echo "########################################################################" >> $detail_file
		
		echo "kw1: 'alter table set location', auto updated to 'clsfdworkingROOT'" >> $detail_file
		echo "kw2: 'clsfd.', auto updated 'clsfd.' to 'clsfd_'" >> $detail_file
		echo "kw3: 'DW_CURRENCIES|DW_DAILY_EXCHANGE_RATES|DW_CALENDAR|DW_CAL_DT', need update 'clsfd_working' to 'clsfd_tables'"  >> $detail_file
		echo "kw4: scientific notatio 'E1X', need update by case"  >> $detail_file
		echo "kw5: 'from_utc_timestamp' or 'to_utc_timestamp' or 'from_unixtime', need update by case"  >> $detail_file
		echo "kw6: 'create view', need update to 'create or replace temporary view'"  >> $detail_file
		echo "kw7: 'yyyy-MM-dd-HH.mm.ss', need update by case"  >> $detail_file
		echo "kw8: 'is_user', need check the parameter and value in yaml"  >> $detail_file
		echo "kw9: 'add jar' or 'add file', need update by case"  >> $detail_file
		echo "kw10: 'lateral view' along with 'create view', need update by case"  >> $detail_file
				
		echo "########################################################################" >> $detail_file
		echo "#***************************SQL FILE NEED UPDATE************************"  >> $detail_file
		echo "#**************notice:kw1 and kw2 auto updated by script****************"  >> $detail_file
		echo "########################################################################" >> $detail_file
		if [ -e ${output_file} ];then
			for i in `cat ${output_file} |grep ": kw"|grep -v "kw1"|grep -v "kw2"|awk -F ':' '{print $1}'|sort|uniq`;
			do
				kw_list=""
				for kw in `cat ${output_file} |grep "${i}"| awk -F' : ' '{print $2}'|grep -v "^$"`;
				do 
					if [ "${kw}" == "kw1" -o "${kw}" == "kw2" ];then
						continue
					fi
					kw_list=${kw_list}" "${kw}
				done
				echo ${i}: ${kw_list} >> ${detail_file}
			done
		fi
		
		echo "Info: proceed generate sql list which need to update,check at ${work_home}/${detail_file}" 
		
	fi
else
	echo "Error: downloaded sql file cnt is less than 0 means download failed"
	exit 2
fi

