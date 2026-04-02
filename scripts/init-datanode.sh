#!/bin/bash
set -e

# Ensure data directory exists with proper permissions
mkdir -p /opt/hadoop/data/dataNode
chmod 755 /opt/hadoop/data/dataNode

echo "Starting DataNode..."
hdfs datanode