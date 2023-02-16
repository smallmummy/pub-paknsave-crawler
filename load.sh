#!/bin/sh

while getopts 'd:' OPT; do
	case $OPT in
	d)
		cur_date="$OPTARG"
		;;
	?)
		echo "Usage: $(basename $0) [-d] date"
		exit
		;;
	esac
done

PID_FILE=".load.pid"
IS_EXISTED=0
MongoDB_conn="127.0.0.1:27017"
MongoDB_db="pak"
MongoDB_tal_store_loaded="store_loaded"
MongoDB_tbl_product_loaded="paknsave"
MongoDB_path="/usr/bin"
ROOT_PATH="/home/ubuntu/vc_code/paknsave"
FILE_IN_PATH="${ROOT_PATH}/output"
FILE_DONE_PATH="${ROOT_PATH}/done"
FILE_TEMP_PATH="${ROOT_PATH}/temp"
SHELL_FOLDER=${PWD}
LOG_PATH="${ROOT_PATH}/log"

tbl_store_catalog_md5="store_catalog_md5"
tbl_store_catalog="store_catalog"

tbl_store_products_md5="store_products_md5"
tbl_products="products"

# just for test
#FILE_IN_PATH="/home/ubuntu/vc_code/paknsave/waiting"
#FILE_DONE_PATH="/home/ubuntu/vc_code/paknsave/done"
#FILE_TEMP_PATH="/home/ubuntu/vc_code/paknsave/temp"
#cur_date="2019-08-28"


if [ -z $cur_date ]; then
	cur_date=`date "+%Y-%m-%d"`
fi


LOG_FILE="loading_${cur_date}.log"

echo
echo >> ${LOG_PATH}/${LOG_FILE}

if [ -f ${PID_FILE} ];then
	pid_no=`cat ${PID_FILE}`
	re=`ps -ef | awk '{print $2}' | grep ${pid_no}`
	if [ $re ];then
		#echo "-${re}-"
		IS_EXISTED=1
	else
		IS_EXISTED=0
	fi
else
	IS_EXISTED=0
fi


if [ ${IS_EXISTED} -eq 1 ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the last process(${pid_no}) didnt finished yet, exit!"
	exit
else
	echo $$ > ${PID_FILE}
	echo "[`date +\"%Y-%m-%d %T\"`] no previous process, now doing the data of ${cur_date}...., detail log output to ${LOG_PATH}/${LOG_FILE}"
fi


DB_pid=`ps -ef | grep "${MongoDB_path}/mongod" | grep -v grep | awk '{print $2}'`
if [ ${DB_pid} ];then
    echo "[`date +\"%Y-%m-%d %T\"`] passed the DB check, DB is alive..."
else
    echo "[`date +\"%Y-%m-%d %T\"`] the DB is not alive, exit loading...."
    exit
fi




if [ ! -d ${FILE_IN_PATH} ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the output dictory:${FILE_IN_PATH} didnt exist,exit!"
	exit
fi

if [ ! -d ${FILE_DONE_PATH} ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the done dictory:${FILE_DONE_PATH} didnt exist,creat it now....!"
	mkdir ${FILE_DONE_PATH}
fi

if [ ! -d ${FILE_TEMP_PATH} ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the temp dictory:${FILE_TEMP_PATH} didnt exist,creat it now....!"
	mkdir ${FILE_TEMP_PATH}
fi


rm ${FILE_TEMP_PATH}/*_tmp 2>/dev/null


dummy_file="dummy_${cur_date}.done"

if [ ! -f ${FILE_IN_PATH}/${dummy_file} ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the dummy file for today:${FILE_IN_PATH}/${dummy_file} didnt exist, treated finished already,now exit..."
	exit
fi


if [ ! -f ${FILE_IN_PATH}/StoreList_${cur_date}.json ];then
	echo "[`date +\"%Y-%m-%d %T\"`] the store info file:${FILE_IN_PATH}/StoreList_${cur_date}.json didnt exist, exit!"
	exit
fi



echo "[`date +\"%Y-%m-%d %T\"`] going for data of ${cur_date}...."  >> ${LOG_PATH}/${LOG_FILE}


echo "[`date +\"%Y-%m-%d %T\"`] begin to process data of store...." >> ${LOG_PATH}/${LOG_FILE}
for line in `ls ${FILE_IN_PATH}/*${cur_date}.store.json`
do

	filename=`echo $(basename $line)`
	echo "[`date +\"%Y-%m-%d %T\"`] now processing ${line}...." >> ${LOG_PATH}/${LOG_FILE}
	store_id=`echo ${filename} | sed 's/\./_/g' | awk -F_ '{print $1}'`
	store_date=`echo ${filename} | sed 's/\./_/g' | awk -F_ '{print $2}'`

    `awk -F\| '{print $1}' ${line} > ${FILE_TEMP_PATH}/${filename}_md5_temp`

	store_catalog_md5=`md5sum ${FILE_TEMP_PATH}/${filename}_md5_temp | awk '{print $1}' `

	record="{\"store_id\":\"${store_id}\",\"store_date\":\"${store_date}\",\"store_catalog_md5\":\"${store_catalog_md5}\"}"
	echo "[`date +\"%Y-%m-%d %T\"`] plan to insert:${record} into DB...." >> ${LOG_PATH}/${LOG_FILE}

	echo "conn = new Mongo(\"${MongoDB_conn}\");db = conn.getDB(\"${MongoDB_db}\");" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp
	echo "var p=${record}" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp
	echo "db.${tbl_store_catalog_md5}.insert(p)" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp


	exe_out="`${MongoDB_path}/mongo ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp  2>&1`"
	#echo ${exe_out}
	any_err=`echo ${exe_out} | grep -i error`

	if [ -n "`echo ${exe_out} | grep -i error`" ] ; then
		echo "[`date +\"%Y-%m-%d %T\"`] [ERROR] there is the error!!!!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] ${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		exit
	else
		echo "[`date +\"%Y-%m-%d %T\"`] store catalog md5 info inserted successfully!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] here is the detail :${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		mv ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp ${FILE_DONE_PATH}/${filename}_md5_loading.js
		rm ${FILE_TEMP_PATH}/${filename}_md5_temp
	fi

	echo >> ${LOG_PATH}/${LOG_FILE}
	echo "[`date +\"%Y-%m-%d %T\"`] inserting all data for store catalog into DB...." >> ${LOG_PATH}/${LOG_FILE}
	`sed 's/}|/,"date":"/' ${line} | sed 's/$/"}/'  > ${FILE_TEMP_PATH}/${filename}_tmp `

	exe_out="`${MongoDB_path}/mongoimport --db ${MongoDB_db} --collection ${tbl_store_catalog} --file ${FILE_TEMP_PATH}/${filename}_tmp 2>&1`"

	if [ -n "`echo ${exe_out} | grep -i error`" ] ; then
		echo "[`date +\"%Y-%m-%d %T\"`] [ERROR] there is the error!!!!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] ${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		exit
	else
		echo "[`date +\"%Y-%m-%d %T\"`] all data dumped successfully!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] here is the detail :${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		mv ${line} ${FILE_DONE_PATH}/${filename}
		rm ${FILE_TEMP_PATH}/${filename}_tmp
	fi

done




for line in `ls ${FILE_IN_PATH}/*${cur_date}.pro.json`
do

	filename=`echo $(basename $line)`
	echo "[`date +\"%Y-%m-%d %T\"`] now processing ${line}...." >> ${LOG_PATH}/${LOG_FILE}
	store_id=`echo ${filename} | sed 's/\./_/g' | awk -F_ '{print $1}'`
	store_date=`echo ${filename} | sed 's/\./_/g' | awk -F_ '{print $2}'`
	store_name=`grep ${store_id} ${FILE_IN_PATH}/StoreList_${cur_date}.json | awk -F'"' '{print $8}'`
	
	
	echo "[`date +\"%Y-%m-%d %T\"`] here is the file info: store id - ${store_id},store name - ${store_name},store date - ${store_date}" >> ${LOG_PATH}/${LOG_FILE}

    `awk -F\| '{print $1}' ${line} > ${FILE_TEMP_PATH}/${filename}_md5_temp`

	store_products_md5=`md5sum ${FILE_TEMP_PATH}/${filename}_md5_temp | awk '{print $1}' `

	record="{\"store_id\":\"${store_id}\",\"store_date\":\"${store_date}\",\"store_products_md5\":\"${store_products_md5}\"}"
	echo "[`date +\"%Y-%m-%d %T\"`] plan to insert:${record} into DB...." >> ${LOG_PATH}/${LOG_FILE}

	echo "conn = new Mongo(\"${MongoDB_conn}\");db = conn.getDB(\"${MongoDB_db}\");" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp
	echo "var p=${record}" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp
	echo "db.${tbl_store_products_md5}.insert(p)" >> ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp


	exe_out="`${MongoDB_path}/mongo ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp  2>&1`"
	#echo ${exe_out}
	any_err=`echo ${exe_out} | grep -i error`
	
	
	if [ -n "`echo ${exe_out} | grep -i error`" ] ; then
		echo "[`date +\"%Y-%m-%d %T\"`] [ERROR] there is the error!!!!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] ${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		exit
	else
		echo "[`date +\"%Y-%m-%d %T\"`] brief info inserted successfully!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] here is the detail :${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		mv ${FILE_TEMP_PATH}/${filename}_md5_loading.js_tmp ${FILE_DONE_PATH}/${filename}_md5_loading.js
		rm ${FILE_TEMP_PATH}/${filename}_md5_temp
	fi



	echo >> ${LOG_PATH}/${LOG_FILE}
	echo "[`date +\"%Y-%m-%d %T\"`] inserting all data into DB...." >> ${LOG_PATH}/${LOG_FILE}
	`sed 's/}|/,"md5":"/' ${line} | sed 's/$/"}/' | sed 's/|/","date":\"/'  > ${FILE_TEMP_PATH}/${filename}_tmp `


	exe_out="`${MongoDB_path}/mongoimport --db ${MongoDB_db} --collection ${tbl_products} --file ${FILE_TEMP_PATH}/${filename}_tmp 2>&1`"
	
	if [ -n "`echo ${exe_out} | grep -i error`" ] ; then
		echo "[`date +\"%Y-%m-%d %T\"`] [ERROR] there is the error!!!!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] ${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		exit
	else
		echo "[`date +\"%Y-%m-%d %T\"`] all data dumped successfully!" >> ${LOG_PATH}/${LOG_FILE}
		echo "[`date +\"%Y-%m-%d %T\"`] here is the detail :${exe_out}" >> ${LOG_PATH}/${LOG_FILE}
		mv ${line} ${FILE_DONE_PATH}/${filename}
		rm ${FILE_TEMP_PATH}/${filename}_tmp
	fi	
	
done



mv ${FILE_IN_PATH}/StoreList_${cur_date}.json ${FILE_DONE_PATH}/StoreList_${cur_date}.json
mv ${FILE_IN_PATH}/${dummy_file} ${FILE_DONE_PATH}/${dummy_file}


#`tar  -Pzcvf ${FILE_DONE_PATH}/${cur_date}.tar.gz ${FILE_DONE_PATH}/*${cur_date}*  --remove-files`


echo "[`date +\"%Y-%m-%d %T\"`] begin to tar files which were done....." >> ${LOG_PATH}/${LOG_FILE}
cd ${FILE_DONE_PATH}
tar -zcvf ${cur_date}.tar.gz *${cur_date}* >> ${LOG_PATH}/${LOG_FILE}
if [ $? -ne 0 ];then
	echo "[`date +\"%Y-%m-%d %T\"`] ERROR: failed on the tar, so didnt delete the source files" >> ${LOG_PATH}/${LOG_FILE}
else
	echo "[`date +\"%Y-%m-%d %T\"`] tar finished!" >> ${LOG_PATH}/${LOG_FILE}
fi

echo "[`date +\"%Y-%m-%d %T\"`] finished all process, exit!" >> ${LOG_PATH}/${LOG_FILE}
