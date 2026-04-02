#!/bin/bash
set -e

# Ensure capacity-scheduler.xml exists
if [ ! -f /opt/hadoop/etc/hadoop/capacity-scheduler.xml ]; then
    echo "Creating capacity-scheduler.xml..."
    cat <<EOF > /opt/hadoop/etc/hadoop/capacity-scheduler.xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>yarn.scheduler.capacity.root.queues</name>
        <value>default</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.capacity</name>
        <value>100</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.maximum-capacity</name>
        <value>100</value>
    </property>

    <property>
        <name>yarn.scheduler.capacity.root.default.state</name>
        <value>RUNNING</value>
    </property>
</configuration>
EOF
fi

echo "Starting YARN ResourceManager..."
yarn --daemon start resourcemanager

# Keep container running
tail -f /dev/null