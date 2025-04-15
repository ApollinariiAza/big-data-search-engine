#!/bin/bash
echo "MapReduce index initiation"

INPUT_PATH="/index/data"
OUTPUT_PATH="/tmp/index"

hdfs dfs -rm -r $OUTPUT_PATH

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming*.jar \
  -input $INPUT_PATH \
  -output $OUTPUT_PATH \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -file /app/mapreduce/mapper1.py \
  -file /app/mapreduce/reducer1.py
