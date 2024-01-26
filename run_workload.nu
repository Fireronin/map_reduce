cd map_reduce
cargo build
cd ..



let workers = kubectl get pods -l app=worker -o jsonpath="{.items[*].metadata.name}"
let workers_list = $workers | split row " "
echo "Workers found" 
echo ($workers_list)

let master_pod =  kubectl get pods -l app=master -o jsonpath='{.items[*].metadata.name}'

echo "Master pod: " | append $master_pod | str join ""

let start_port = 10070

def background [cmd: string] {
    echo $"Running command: ($cmd)"
  bash -c $cmd
}

let workers_ips = kubectl get pods -l app=worker -o jsonpath='{.items[*].status.podIP}' | split row " "
# add ports
let workers_ips = $workers_ips | enumerate | each {|e| 
    let sum_port = $start_port + $e.index
    $"($e.item):($sum_port)"
}
echo $workers_ips

mut i = 0
for worker in $workers_list {
    echo $"Preparing worker ($worker)"
    let total_port = $start_port + $i

    try {kubectl exec ($worker) -- pkill -f map_reduce}

    kubectl cp ./map_reduce/target/debug/map_reduce $"($worker):/home/map/map_reduce"
    let pod_ip  = kubectl get pods -l app=worker -o $'jsonpath="{.items[($i)].status.podIP}"'
    background $"kubectl exec ($worker) -- /bin/sh -c 'export WORKER_URL=($pod_ip):($total_port) && /home/map/map_reduce' &" 
    $i += 1
}

echo "Finished port forwarding"


echo "Workers ips: "
echo $workers_ips

$workers_ips | str join "," | save -f workers_urls 

kubectl cp ./map_reduce/target/debug/map_reduce $"($master_pod):/home/map/map_reduce"
kubectl cp ./workers_urls $"($master_pod):/home/map/workers_urls" -c master

kubectl exec $master_pod -- /home/map/map_reduce

