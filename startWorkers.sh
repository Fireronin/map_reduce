#!/bin/bash

# Set the number of pods
N=5

# Create the deployment
kubectl apply -f ./deployment.yaml

# Wait for the pods to be ready
sleep 5

# Get the names of the pods
worker_pods=$(kubectl get pods -l app=worker -o jsonpath="{.items[*].metadata.name}")
master_pod=$(kubectl get pods -l app=master -o jsonpath="{.items[*].metadata.name}")

worker_pod =

START_PORT=9690

# Loop over the pods
i=0
for pod in $worker_pods; do
  echo "Pod: $pod"
  # Copy the file
  kubectl cp text.txt $pod:/text.txt
#kubectl cp text.txt  map-reduce-559658b776-g565k:/text.txt
  # Run cat
  kubectl exec $pod -- mv /text.txt /text2.txt
    # kubectl exec map-reduce-559658b776-g565k -- "mv /text.txt /text2.txt"
  # Forward the port
  kubectl port-forward $pod $(expr $START_PORT + $i):9000 &

  # Increment the counter
  i=$(expr $i + 1)
done

# kubectl get pods -l app=worker -o jsonpath='{.items[*].status.podIP}'