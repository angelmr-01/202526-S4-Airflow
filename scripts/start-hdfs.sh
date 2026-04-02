#!/bin/bash
set -e

# Ensure data directory exists with proper permissions
mkdir -p /opt/hadoop/data/nameNode
chmod 755 /opt/hadoop/data/nameNode

# Format NameNode if not already formatted
if [ ! -d "/opt/hadoop/data/nameNode/current" ]; then
    echo "Formatting NameNode..."
    echo 'Y' | hdfs namenode -format
fi

echo "Starting NameNode..."
hdfs namenode