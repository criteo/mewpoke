#!/bin/bash
apt-get update
apt-get install -y netcat
./run.sh &
echo "Waiting grafana to launch on 3000..."
while ! nc -z localhost 3000 ; do 
 sleep 1
done

curl -u admin:password -H 'Content-Type: application/json'  -X POST http://localhost:3000/api/datasources --data-binary @/install/data-source-prometheus.json
curl -u admin:password -H 'Content-Type: application/json'  -X POST http://localhost:3000/api/dashboards/db --data-binary @/install/mewpoke-dashboard.json

# Need to restart process without & in order to not finish the container
pkill -u grafana
./run.sh
