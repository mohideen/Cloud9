#!/bin/bash

if [ $# -ne 3 ]
then
  echo "Usage: logJob <job_ID> <log_to_location> <num_reducers>"
  exit 1
fi

jobID=$1
saveTo=$2
numReducers=$3
jobtracker="localhost"
idNumber=`echo $jobID | grep -o '[0-9]\{12\}_[0-9]\{4\}'`

#Create output directory
mkdir $saveTo
wget -O $saveTo/jobdetails.html "http://$jobtracker:50030/jobdetails.jsp?jobid="$jobID"&refresh=0"
wget -O $saveTo/jobtasks-Map.html "http://$jobtracker:50030/jobtasks.jsp?jobid="$jobID"&type=map&pagenum=1&state=completed"
wget -O $saveTo/jobtasks-Reduce.html "http://$jobtrackerv:50030/jobtasks.jsp?jobid="$jobID"&type=reduce&pagenum=1&state=completed"

loop=0
while [ $loop -lt $numReducers ]
do
  formattedCount=`printf "%05d" "$loop"`
  wget -O $saveTo/taskstats$formattedCount.html "http://$jobtracker:50030/taskstats.jsp?tipid=task_"$idNumber"_r_0"$formattedCount
  loop=`expr $loop + 1`
done
