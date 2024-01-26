#!/bin/zsh
# Set the number of pods
N=5

# Create the deployment
kubectl apply -f ./deployment.yaml

# Wait for the pods to be ready
sleep 1

# Get the names of the pods
worker_pods=$(kubectl get pods -l app=worker -o jsonpath="{.items[*].metadata.name}")
master_pod=$(kubectl get pods -l app=master -o jsonpath="{.items[*].metadata.name}" | cut -d' ' -f1)

worker_pod=${worker_pods}

START_PORT=10000

# Workers IPs
WORKERS_IPS=($(kubectl get pods -l app=worker -o jsonpath='{.items[*].status.podIP}'))

# Remove the first element
#WORKERS_IPS=("${WORKERS_IPS[@]:1}")

# Append the port
WORKERS_IPS=("${WORKERS_IPS[@]/%/:9000}")

# Loop over the pods
i=0
for pod in "${worker_pods[@]}"; do
  echo "Pod: $pod"
  # Copy the file
  # kubectl cp ./target/debug/map_reduce map-reduce-675ddd87bd-vvrcw:/map_reduce
  # kubectl cp ./target/debug/map_reduce $worker_pod:/home/map/map_reduce
  # Chmod +x
  # kubectl exec $pod -- /bin/sh -c "chmod +x /home/map/map_reduce"

  # Forward the port
  kubectl port-forward $pod $(($START_PORT + $i)):9000 &
  echo "Starting the worker"
  #kubectl exec $pod -- /home/map/map_reduce/target/debug/map_reduce
  #kubectl exec $worker_pod -- /home/map/map_reduce/target/debug/map_reduce
  # pod IP
  pod_ip=$(kubectl get pods -l app=worker -o jsonpath="{.items[$i].status.podIP}")

  kubectl exec $pod -- /bin/sh -c "export WORKER_URL=$pod_ip:9000 && /home/map/map_reduce/target/debug/map_reduce" &
  # Increment the counter
  ((i++))
done

# echo 
echo "Starting the master"

sleep 2
workers_url=$(IFS=','; echo "${WORKERS_IPS[*]}")

# save this into workers_urls file for master to read
echo $workers_url > workers_urls

# copy the file to master
kubectl cp ./workers_urls $master_pod:/home/map/workers_urls -c master

# add map_reduce to path
kubectl exec $master_pod -- /home/map/map_reduce/target/debug/map_reduce

# Start the master
# kubectl exec $pods[0] -- /bin/sh -c "export ROLE=master && export WORKERS_URL='/workers_urls' && /map_reduce"
# kubectl exec $pods[0] -- /bin/sh -c 'echo $PATH'
# kubectl exec $pods[0] -- /bin/sh -c '/bin/sh -c map_reduce'

# kubectl exec $worker_pod -- /bin/sh -c "export WORKER_URL=10.1.0.75:9000 && /home/map/map_reduce/target/debug/map_reduce " 