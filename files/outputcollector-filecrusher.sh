#!/bin/bash
#Script that collects batchdatagen outputs files: merges small
#into to larger file, and copies larger files as it is. The script loops
#through multiple folders with the same base source path (i.e. created by
#the batchdatagen script), and writes the output to a single folder. 
#The collected files can be used as input to the tuple packer program.
#This script requires that number of files to be processed in each folder is same.

#Script Run Configuration
baseSourcePath=data/set12.
destFolderPath=batch2/set12/combined
start=0
end=100
statusLogFile=merge.log

echo "$(date +%H:%M:%S): Started" >> $statusLogFile
START_TIME=$SECONDS
count=$start
while [ $count -le $end ]
do
  hadoop fs -rmr $baseSourcePath$count/_logs
  hadoop fs -rmr $baseSourcePath$count/_SUCCESS
  options="--input-format=text --output-format=text --compress=none --verbose"
  echo "hadoop jar filecrush-2.0.jar crush.Crush $options $baseSourcePath$count $destFolderPath $(date +%Y%m%d%H%M%S)  "
  hadoop jar filecrush-2.0.jar crush.Crush $options $baseSourcePath$count $destFolderPath $(date +%Y%m%d%H%M%S)  
  count=`expr $count + 1`
done
ss=$(($SECONDS - $START_TIME))
mm=$(($ss/60))
hh=$(($mm/60))
ss=$(($ss%60))
mm=$(($mm%60))
echo "$(date +%H:%M:%S): - Took Total "`printf "%02d:%02d:%02d" "$hh" "$mm" "$ss"` >> $statusLogFile
