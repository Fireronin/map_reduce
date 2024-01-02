# Set the number of pods
$N = 5

# Create the deployment
kubectl apply -f ./deployment.yaml

# Wait for the pods to be ready
Start-Sleep -Seconds 1

# Get the names of the pods
$worker_pods = (kubectl get pods -l app=worker -o jsonpath="{.items[*].metadata.name}").Split(' ')
$master_pod = (kubectl get pods -l app=master -o jsonpath="{.items[*].metadata.name}").Split(' ')[0]

$worker_pod = $worker_pods[0]

$START_PORT = 10000

# Workers IPs
$WORKERS_IPS = (kubectl get pods -l app=worker -o jsonpath='{.items[*].status.podIP}').Split(' ')

# Remove the first element
$WORKERS_IPS = $WORKERS_IPS | Select-Object -Skip 1

# Append the port
$WORKERS_IPS = $WORKERS_IPS | ForEach-Object { $_ + ':9000' }

# Loop over the pods
$i = 0
foreach ($pod in $worker_pods) {
  Write-Host "Pod: $pod"
  # Copy the file # kubectl cp .\target\debug\map_reduce map-reduce-675ddd87bd-vvrcw:/map_reduce
  # kubectl cp .\target\debug\map_reduce ($pod + ':/home/map/map_reduce')
  # # Chmod +x
  # kubectl exec $pod -- /bin/sh -c "chmod +x /home/map/map_reduce"

  # Forward the port
  Start-Process -NoNewWindow -FilePath "kubectl" -ArgumentList "port-forward $pod $($START_PORT + $i):9000"
  echo "Starting the worker"
  kubectl exec $pod -- /home/map/map_reduce/target/debug/map_reduce


  # Increment the counter
  $i = $i + 1
}

# echo 
echo "Starting the master"

Start-Sleep -Seconds 2
$workers_url = $($WORKERS_IPS -join ',')

# save this into workers_urls file for master to read
echo $workers_url > workers_urls

# copy the file to master
kubectl cp .\workers_urls ${master_pod}:/home/map/workers_urls

# add map_reduce to path
kubectl exec $master_pod -- /home/map/map_reduce/target/debug/map_reduce

# # Start the master
# kubectl exec $pods[0] -- /bin/sh -c "export ROLE=master && export WORKERS_URL='/workers_urls' && /map_reduce"
# kubectl exec $pods[0] -- /bin/sh -c 'echo $PATH'
# kubectl exec $pods[0] -- /bin/sh -c '/bin/sh -c map_reduce'