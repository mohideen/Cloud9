#!/bin/bash
#Script to set datagen environmental variables
export datagenjar=/pig/pigperf.jar
export zipfjar=/pig/lib/sdsuLibJKD12.jar
export pigjar=/pig/lib/pig-0.9.0-core.jar
export conf_file=/custom.xml
export HADOOP_CLASSPATH=$pigjar:$zipfjar:$datagenjar
export RUNDATAGEN=hadoop\ jar\ $datagenjar\ org.apache.pig.test.utils.datagen.DataGenerator\ -libjars\ $zipfjar\ -conf\ $conf_file
