#!/bin/bash
#The data generator was very slow while generating zipf data. 
#The below scripts were used to generate the Zipf datasets in batch mode.

source datagen.sh
i=1
while [ $i -le 100 ]
do
  START_TIME=$SECONDS
  args="-m 500 -s $'\t' -f data/set12.$i -r 10000000 i:1:100000000:z:0 i:1:10000:u:0 i:1:100000:u:0"
  echo "$RUNDATAGEN $args "
  $RUNDATAGEN $args 
  ss=$(($SECONDS - $START_TIME))
  mm=$(($ss/60))
  hh=$(($mm/60))
  ss=$(($ss%60))
  mm=$(($mm%60))
  echo "$(date +%H:%M:%S): Done $i - Took "`printf "%02d:%02d:%02d" "$hh" "$mm" "$ss"` >> batchgen.log
  i=`expr $i + 1`
done
