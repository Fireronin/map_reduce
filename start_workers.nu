docker build -t map-reduce:latest .
try { kubectl delete deployment worker }
try { kubectl delete deployment master }
# remove all pods
kubectl delete pods --all --grace-period=0 --force

kubectl apply -f deployment.yaml

echo "Please wait for the pods to be ready (5-10 seconds should be enough)"
echo "Run workload using: nu run_workload.nu"