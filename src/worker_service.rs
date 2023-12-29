use tonic::transport::Channel;
use tonic::{transport::Server, Request, Response, Status};
use tokio::signal;
use map_reduce::map_reduce_server::*;
use map_reduce::*;
use crate::map_reduce_utils::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Notify};
use std::net::SocketAddr;
use serde::{Deserialize, Serialize};
use rand::Rng;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use map_reduce::map_reduce_client::MapReduceClient;
use tokio::io::{AsyncReadExt, AsyncWriteExt};



pub fn multiply_by_2(x: &KeyValue) -> KeyValue {
    KeyValue {
        key: x.key,
        value: x.value * 2,
    }
}

pub fn sum(x: &KeyValue, y: &KeyValue) -> KeyValue {
    KeyValue {
        key: x.key,
        value: x.value + y.value,
    }
}


pub struct MapReduceWorker {
    context : Context,
}

impl MapReduceWorker {
    fn new(context: &Context) -> Self {
        MapReduceWorker {
            context: context.clone(),
        }
    }
}

#[tonic::async_trait]
impl MapReduce for MapReduceWorker {

    async fn map_chunk(
        &self,
        request: Request<MapRequest>,
    ) -> Result<Response<MapReply>, Status> {

        let inner_request = request.into_inner();
        let function = inner_request.function.as_str();
        let data = &inner_request.data;
        let output = self.context.map_function_rayon_serialized(function,data);
        match output {
            Some(x) => {
                let reply = map_reduce::MapReply {
                    data: x,
                };
                Ok(Response::new(reply))
            },
            None => {
                Err(Status::not_found("Function not found"))
            }
        }
    }

    async fn reduce_chunk(
        &self,
        request: Request<MapRequest>,
    ) -> Result<Response<MapReply>, Status> {

        let inner_request = request.into_inner();
        let function = inner_request.function.as_str();
        let data = &inner_request.data;
        print!("reduce_chunk");
        let output = self.context.reduce_function_rayon_serialized(function,data);
        match output {
            Some(x) => {
                let reply = map_reduce::MapReply {
                    data: x,
                };
                Ok(Response::new(reply))
            },
            None => {
                Err(Status::not_found("Function not found"))
            }
        }
    }
}

async fn notify_master(worker_addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    // Establish a connection to the master to send the worker's address
    let master_addr: Result<SocketAddr, _> = "[::1]:50052".parse();
    let master_addr = master_addr?;

    let mut stream = tokio::net::TcpStream::connect(master_addr).await?;

    // Send the worker's address to the master
    let worker_addr_str = format!("{}", worker_addr);
    stream.write_all(worker_addr_str.as_bytes()).await?;

    Ok(())
}
pub async fn do_worker(context: &Context) -> Result<(), Box<dyn std::error::Error>> {
    // Attempt to find a free address
    let addr = find_free_address().await?;

    println!("Found free address: {}", addr);

    // Notify the master about the worker's address
    notify_master(addr.parse().unwrap()).await?;

    println!("Notified master about the worker's address");

    let greeter = MapReduceWorker::new(&context);

    let shutdown = Notify::new();

    let ctrl_c = tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
        shutdown.notify_one();
    });

    println!("Listening on {}", addr);

    let server = Server::builder()
        .add_service(MapReduceServer::new(greeter))
        .serve(addr.parse().unwrap());

    tokio::select! {
        _ = ctrl_c => {
            println!("Ctrl-C event received, shutting down");
        },
        _ = server => {
            println!("Server stopped");
        },
    }

    Ok(())
}

async fn find_free_address() -> Result<String, Box<dyn std::error::Error>> {
    let mut rng = rand::thread_rng();

    // Attempt to find a free address in the range [50000, 60000)
    for _ in 0..1000 {
        let port: u16 = rng.gen_range(50000..60000);
        let addr_str = format!("[::1]:{}", port);

        // Check if the address is free
        if is_address_free(&addr_str).await? {
            return Ok(addr_str);
        }
    }

    Err("Unable to find a free address".into())
}

async fn is_address_free(addr: &str) -> Result<bool, Box<dyn std::error::Error>> {
    match TcpListener::bind(addr).await {
        Ok(_) => Ok(true),  // Address is free
        Err(_) => Ok(false), // Address is in use
    }
}

#[derive(Serialize, Deserialize, Debug, Clone,Default)]
pub struct KeyValue {
    pub key: i32,
    pub value: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    task_id: usize,
    //user_address: SocketAddr,
    data: Vec<KeyValue>,
}

pub async fn do_master(context: &Context) -> Result<(), Box<dyn std::error::Error>> {
    let addr_workers: SocketAddr = "[::1]:50052".parse().unwrap();
    let addr_jobs: SocketAddr = "[::1]:50051".parse().unwrap();

    let listener_workers = TcpListener::bind(&addr_workers).await?;
    let listener_jobs = TcpListener::bind(&addr_jobs).await?;

    let (worker_sender, _) = mpsc::channel::<SocketAddr>(20);
    let (job_sender, _) = mpsc::channel::<Job>(10);
    let worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let worker_listener = tokio::spawn(listen_for_workers(
        listener_workers,
        worker_sender,
        Arc::clone(&worker_addresses),
    ));
    let job_listener = tokio::spawn(listen_for_jobs(
        listener_jobs,
        job_sender,
        Arc::clone(&worker_addresses),
    ));

    let ctrl_c = tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
    });

    tokio::select! {
        _ = ctrl_c => {
            println!("Ctrl-C event received, shutting down");
        },
        _ = worker_listener => {
            println!("Worker listener stopped");
        },
        _ = job_listener => {
            println!("Job listener stopped");
        },
    }

    // Here you can handle any cleanup or additional logic before exiting the master

    Ok(())
}

/*async fn listen_for_workers(
    listener: TcpListener,
    sender: mpsc::Sender<SocketAddr>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) {
    loop {
        tokio::select! {
            result_worker = listener.accept() => {
                match result_worker {
                    Ok((stream, addr)) => {
                        let mut addr_str = addr.port().to_string();
                        addr_str = format!("http://{}", addr_str);
                        println!("{}", addr_str);
                        let client = MapReduceClient::connect(addr_str).await;
                        println!("connected to the master");

                        match client {
                            Ok(my_client) => {
                                // Insert the worker address with task_id 0 and availability true
                                worker_addresses
                                    .lock()
                                    .unwrap()
                                    .insert(addr, (my_client, true));

                                // Send the worker's address to the main thread
                                sender
                                    .send(addr)
                                    .await
                                    .expect("Failed to send worker address");
                                println!("Worker registered: {:?}", addr);
                            }
                            Err(e) => {
                                eprintln!("Error connecting to worker {}: {:?}", addr, e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error accepting worker connection: {:?}", e);
                    }
                }
            }
        }
    }
}*/
/*async fn listen_for_workers(
    listener: TcpListener,
    sender: mpsc::Sender<SocketAddr>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) {
    loop {
        tokio::select! {
            result_worker = listener.accept() => {
                match result_worker {
                    Ok((stream, addr)) => {
                        // Read the worker's server address from the stream
                        let worker_addr_str = read_socket_addr_from_stream(stream).await;

                        

                        match worker_addr_str {
                            Ok(worker_addr) => {
                                println!("Worker connected: {:?}", worker_addr);
                                // Convert the SocketAddr to a string
                                let worker_addr_str = format!("http://{:?}", worker_addr);
                                println!("{:?}",worker_addr_str);
                                // Convert the string to the expected type
                                //let worker_addr_str_ref: &str = &worker_addr_str;

                                /*let client = MapReduceClient::connect(worker_addr_str).await;
                                println!("Connected to the master");
                                //let client = MapReduceClient::connect(worker_addr.clone()).await;
                                //println!("Connected to the master");

                                match client {
                                    Ok(my_client) => {
                                        // Insert the worker address with task_id 0 and availability true
                                        worker_addresses
                                            .lock()
                                            .unwrap()
                                            .insert(worker_addr.clone(), (my_client, true));

                                        // Send the worker's address to the main thread
                                        sender
                                            .send(worker_addr)
                                            .await
                                            .expect("Failed to send worker address");
                                        println!("Worker registered: {:?}", worker_addr);
                                    }
                                    Err(e) => {
                                        eprintln!("Error connecting to worker {}: {:?}", worker_addr, e);
                                    }
                                } */
                            }
                            Err(e) => {
                                eprintln!("Error reading worker address from stream: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error accepting worker connection: {:?}", e);
                    }
                }
            }
        }
    }
}*/

async fn listen_for_workers(
    listener: TcpListener,
    sender: mpsc::Sender<SocketAddr>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) {
    loop {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let sender_clone = sender.clone();
                let worker_addresses_clone = Arc::clone(&worker_addresses);

                // Spawn a new task to handle the worker connection asynchronously
                tokio::task::spawn(handle_worker_connection(stream, sender_clone, worker_addresses_clone));
            }
            Err(e) => {
                eprintln!("Error accepting worker connection: {:?}", e);
            }
        }
    }
}
async fn handle_worker_connection(
    stream: tokio::net::TcpStream,
    sender: mpsc::Sender<SocketAddr>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) {
    // Read the worker's server address from the stream
    let worker_addr = read_socket_addr_from_stream(stream).await;

    match worker_addr {
        Ok(worker_addr) => {
            println!("Worker connected: {:?}", worker_addr);

            // Convert the SocketAddr to a string
            let worker_addr_str = format!("http://{}", worker_addr);

            let client = MapReduceClient::connect(worker_addr_str).await;
            println!("Connected to the master");

            match client {
                Ok(my_client) => {
                    // Insert the worker address with task_id 0 and availability true
                    worker_addresses
                        .lock()
                        .unwrap()
                        .insert(worker_addr.clone(), (my_client, true));

                    // Send the worker's address to the main thread
                    /*sender
                        .send(worker_addr)
                        .await
                        .expect("Failed to send worker address");
                    println!("Worker registered: {:?}", worker_addr);*/
                }
                Err(e) => {
                    eprintln!("Error connecting to worker {}: {:?}", worker_addr, e);
                }
            }
        }
        Err(e) => {
            eprintln!("Error reading worker address from stream: {:?}", e);
        }
    }
}

async fn read_socket_addr_from_stream(
    mut stream: tokio::net::TcpStream,
) -> Result<SocketAddr, Box<dyn std::error::Error + Send>> {
    // Read the address as a string from the stream
    let mut buffer = Vec::new();
    if let Err(err) = stream.take(256).read_to_end(&mut buffer).await {
        return Err(Box::new(err) as Box<dyn std::error::Error + Send>);
    }

    let addr_str = match String::from_utf8(buffer) {
        Ok(s) => s,
        Err(err) => return Err(Box::new(err) as Box<dyn std::error::Error + Send>),
    };

    // Parse the address
    let addr: SocketAddr = match addr_str.parse() {
        Ok(a) => a,
        Err(err) => return Err(Box::new(err) as Box<dyn std::error::Error + Send>),
    };

    Ok(addr)
}

async fn read_job_data_from_stream(
    mut stream: tokio::net::TcpStream,
) -> Result<Vec<KeyValue>, Box<dyn std::error::Error>> {
    // Read the length of the serialized data
    let mut length_bytes = [0u8; 8];
    stream.read_exact(&mut length_bytes).await?;

    let length = u64::from_le_bytes(length_bytes) as usize;

    // Read the serialized data into a buffer
    let mut buffer = vec![0u8; length];
    stream.read_exact(&mut buffer).await?;

    // Deserialize the buffer into a Vec<KeyValue>
    let job_data: Vec<KeyValue> = bincode::deserialize(&buffer)?;

    Ok(job_data)
}

async fn listen_for_jobs(
    //context: &Context,
    listener: TcpListener,
    sender: mpsc::Sender<Job>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) {
    let mut task_counter = 0;

    loop {
        let result_job = listener.accept().await;

        match result_job {
            Ok((stream, addr)) => {
                let worker_addresses = Arc::clone(&worker_addresses);
                //let context = context.clone();
                let sender = sender.clone();

                tokio::spawn(async move {
                    // Example: Read job data from the stream and send it to the job handler
                    
                    let job_data = match read_job_data_from_stream(stream).await {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("Error reading job data from stream: {:?}", e);
                            return;
                        }
                    };
                    println!("{:?}",job_data);

                    // Choose a free worker from the list
                    let worker_address = loop {
                        // Acquire the lock before accessing the HashMap
                        let mut addresses = worker_addresses.lock().unwrap();
                        if let Some((addr, (client, available))) = addresses
                            .iter_mut()
                            .find(|(_, (_, available))| *available)
                        {
                            *available = false;
                            break addr.clone();
                        } else {
                            //eprintln!("No available worker. Retrying...");
                            // Sleep for 1 second before retrying
                            //tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    };

                    // Register the job with a unique task_id
                    let task_id = task_counter;
                    task_counter += 1;
                    let job = Job {
                        task_id,
                        data: job_data,
                    };

                    // Send the job to the worker for processing
                    let result = process_job(/*&context,*/ job, &worker_address, sender, worker_addresses).await;

                    if let Err(e) = result {
                        eprintln!("Error processing job: {:?}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting job connection: {:?}", e);
            }
        }
    }
}

async fn process_job(
    /*context: &Context,*/
    job: Job,
    worker_address: &SocketAddr,
    sender: mpsc::Sender<Job>,
    worker_addresses: Arc<Mutex<HashMap<SocketAddr, (MapReduceClient<Channel>, bool)>>>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create a new client for each job to avoid data races
    let mut context = Context::new();
    add_function!(context, multiply_by_2);
    add_function_reduce!(context, sum);
    let mut client = if let Some((client, _)) = worker_addresses
        .lock()
        .unwrap()
        .get_mut(worker_address)
    {
        client.clone()
    } else {
        return Err("Worker not found".into());
    };
    let job_data = job.data;
    // Perform the map function
    let map_result = map_distributed!(context, multiply_by_2, job_data, &mut client);
    println!("Map RESPONSE={:?}", map_result);

    // Perform the reduce function
    let reduce_result = reduce_distributed!(context, sum, map_result, &mut client);
    println!("Reduce RESPONSE={:?}", reduce_result);

    // Restore the worker's availability
    /*sender
        .send(Job {
            task_id: job.task_id,
            data: Vec::new(), // Or any placeholder data
        })
        .await
        .expect("Failed to notify job completion");*/

    // Set the worker's availability to true
    worker_addresses
        .lock()
        .unwrap()
        .get_mut(worker_address)
        .unwrap()
        .1 = true;

    Ok(())
}

pub async fn send_job_data_to_listener(/*addr: SocketAddr,*/ job_data: Vec<KeyValue>) -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the listener's address and port
    let addr_jobs: SocketAddr = "[::1]:50051".parse().unwrap();
    let mut stream = TcpStream::connect(addr_jobs).await?;

    // Serialize the Vec<KeyValue> into bytes
    let serialized_data = bincode::serialize(&job_data)?;

    // Send the length of the serialized data as u64
    stream.write_all(&(serialized_data.len() as u64).to_le_bytes()).await?;

    // Send the serialized data
    stream.write_all(&serialized_data).await?;

    Ok(())
}