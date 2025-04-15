#!/bin/bash

INPUT_PATH="/index/data"

# Function to log messages
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to process directory with Python
process_directory() {
    local dir_path="$1"

    log "Processing directory: $dir_path"

    source .venv/bin/activate
    export PYSPARK_DRIVER_PYTHON=$(which python)
    unset PYSPARK_PYTHON

    python -c "
import os
import re
from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession

def create_doc(row):
    os.makedirs('data', exist_ok=True)
    filename = 'data/' + sanitize_filename(str(row['id']) + '_' + row['title']).replace(' ', '_') + '.txt'
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(row['text'])

spark = SparkSession.builder.appName('data preparation').master('local').getOrCreate()
input_folder = '$dir_path'
file_data = []

for filename in os.listdir(input_folder):
    file_path = os.path.join(input_folder, filename)
    if os.path.isdir(file_path): continue
    match = re.match(r'(\d+)_(.+)\\.txt$', filename)
    if match:
        doc_id = match.group(1)
        title = match.group(2).replace('_', ' ')
    else:
        doc_id = str(len(file_data) + 1)
        title = os.path.splitext(filename)[0]
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        file_data.append((doc_id, title, text))
    except Exception as e:
        print(f'Error reading file {filename}: {e}')

df = spark.createDataFrame(file_data, ['id', 'title', 'text'])
df.foreach(create_doc)
df.write.option('sep', '\t').mode('append').csv('/index/data')
spark.stop()
"

    if [ $? -eq 0 ]; then
        log "Python processing successful."

        log "Appending new files to HDFS: /index/data"
        hdfs dfs -put data /
        hdfs dfs -ls /data
        hdfs dfs -ls /index/data

        return 0
    else
        log "Python script failed!"
        return 1
    fi
}


if ! process_directory "/app/data"; then
                log "Failed to process and upload"
                exit 1
fi


# Handle custom path input
if [ "$#" -eq 1 ]; then
    CUSTOM_PATH="$1"

    if [ -e "$CUSTOM_PATH" ]; then
        if [ -d "$CUSTOM_PATH" ]; then
            log "Using internal Python processing..."
            if ! process_directory "$CUSTOM_PATH"; then
                log "Failed to process and upload"
                exit 1
            fi
        elif [ -f "$CUSTOM_PATH" ]; then
            log "Uploading single file to HDFS"
            hdfs dfs -put "$CUSTOM_PATH" /data/
        else
            log "Unsupported file type, using raw HDFS path"
        fi
    else
        log "Path not found locally, assuming HDFS path"
    fi
fi

# Cleanup previous runs
hdfs dfs -rm -r /tmp/index/output1
hdfs dfs -rm -r /tmp/index/output2

log "Indexing with MapReduce from: $INPUT_PATH"

# Ensure scripts are executable
chmod +x $(pwd)/mapreduce/mapper1.py $(pwd)/mapreduce/reducer1.py
chmod +x $(pwd)/mapreduce/mapper2.py $(pwd)/mapreduce/reducer2.py

# First MapReduce job
log "Starting MapReduce Job 1"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=2048 \
    -D mapreduce.reduce.java.opts=-Xmx1800m \
    -mapper ".venv/bin/python mapper1.py" \
    -reducer ".venv/bin/python reducer1.py" \
    -input "$INPUT_PATH" \
    -output /tmp/index/output1

if [ $? -ne 0 ]; then
    echo "Error: First MapReduce job failed"
    exit 1
fi

# Second MapReduce job
log "Starting MapReduce Job 2"
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
    -archives /app/.venv.tar.gz#.venv \
    -D mapreduce.reduce.memory.mb=2048 \
    -D mapreduce.reduce.java.opts=-Xmx1800m \
    -mapper ".venv/bin/python mapper2.py" \
    -reducer ".venv/bin/python reducer2.py" \
    -input "/tmp/index/output1" \
    -output "/tmp/index/output2"

if [ $? -ne 0 ]; then
    echo "Error: Second MapReduce job failed"
    APP_ID=$(yarn application -list | grep "application_" | tail -1 | awk '{print $1}')
    if [ ! -z "$APP_ID" ]; then
        echo "YARN Application ID: $APP_ID"
        echo "Log Snippet:"
        yarn logs -applicationId "$APP_ID" | grep -A 20 "stderr"
    fi
    exit 1
fi

log "Indexing completed successfully!"
log "Index data is now stored in Cassandra."

# Final cleanup
hdfs dfs -rm -r -f /tmp/index/output1
hdfs dfs -rm -r -f /tmp/index/output2
