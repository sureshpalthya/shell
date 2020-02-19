#!/bin/sh
log="/var/log/hadoop/logs/hivehbasecntltbl.log"
echo -e "-------------------------------------------------------------------\n" >> $log
echo `date` >>$log
message="Hive-Hbase Control Table - Unable to Connect "
#message1="SAS-HIVESERVER2-Connection Took $timetaken seconds - Please investigate"
sendTo="suresh@gmail.com"


start=`date "+%s"`

#beeline -u "jdbc:hive2://plsq00037m1.corp.sprint.com:10000/0bq_dca;principal=hive/_HOST@DCAPROD.CORP.SPRINT.COM?hive.execution.engine=tez;tez.queue.name=gg;" -e "show databases"
kinit -kt /etc/security/keytabs/hue.service.keytab hue/<hostname>
beeline -u "jdbc:hive2://<hostname>/default;principal=hive/_HOST@.COM?hive.execution.engine=tez;tez.queue.name=gg;" -e "insert into table hbasehive_dontdrop select * from (select FROM_UNIXTIME(UNIX_TIMESTAMP()), FROM_UNIXTIME(UNIX_TIMESTAMP()), FROM_UNIXTIME(UNIX_TIMESTAMP()),1)a;" >> $log

if [[ $? -eq 1 ]]; then
   errorAlert=true
else
    errorAlert=false
fi

end=`date "+%s"`

timetaken=`expr $end - $start`

message1="Hive Hbase CNTL ALERT- INSERTS ARE SLOW"


echo -e "-----------------------------------------\n" >> $log
echo -e "HIVE HBASE INSERT: Time Taken: $timetaken \n" >> $log
echo -e "\n" >> $log

if [[ $timetaken -gt 59 ]]; then
 connAlert=true
else
  connAlert=false
fi

if [[ $errorAlert == "true" ]];then
 echo -e "$message"|mail -s "HIVE-HBASE-CNTL-TBL-BEELINE-CONN ERROR" $sendTo
elif [[ $connAlert == "true" ]]; then
  echo -e "$message1"|mail -s "HIVE-HBASE-CNTL-TBL-ALERT INSERTS ARE SLOW" $sendTo
else
  echo -e "Connection Test Successful \n" >> $log
fi

echo -e "-----------------------------------------\n" >> $log
echo -e "HIVE Connection: Time Taken: $timetaken \n" >> $log
echo -e "Connection ALERT - $errorAlert \n" >> $log
echo -e "Connection Latency Alert - $connAlert \n" >> $log
echo -e "\n" >> $log

cat default.txt | awk '{print $2}' > default_data.txt

col5=`head -n 1 default_data.txt | wc -w`; for (( i=1; i <= $col5; i++)); do   awk '{printf ("%s%s", tab, $'$i'); tab="\t"} END {print ""}' default_data.txt; done >> mergedoutput.txt



