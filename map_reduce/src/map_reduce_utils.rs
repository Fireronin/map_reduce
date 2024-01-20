use serde::{Serialize, Deserialize};
use tokio::time::{timeout, Duration};
use tonic::transport::Channel;
use std::collections::HashMap;
use map_reduce::*;
use map_reduce::map_reduce_client::MapReduceClient;
use std::any::Any;
use rayon::prelude::*;
use std::sync::Arc;
use std::env;
use crate::worker_service;
pub mod map_reduce {
    tonic::include_proto!("map_reduce");
}



pub fn wrapper_rayon<FROM: Any + for<'de> Deserialize<'de> + Serialize + Sync + Send, TO: Serialize + for<'de> Deserialize<'de> + Send + Sync, F: Fn(&FROM) -> TO + Sync + Send + 'static>(f: Arc<F>,data: &Vec<u8>) -> Vec<u8> {
    let data = bincode::deserialize::<Vec<FROM>>(data).unwrap();
    let result: Vec<_> = data.par_iter()
        .map(|item| {
            f(item)
        })
        .collect();
    bincode::serialize(&result).unwrap()
}

#[macro_export]
macro_rules! add_function {
    ($context:ident, $function:expr) => {
        let f = Arc::new($function);
        $context.functions_map_rayon.insert(stringify!($function).to_string(), Arc::new(move |data| wrapper_rayon(Arc::clone(&f),&data)));
    };
}

pub fn wrapper_rayon_reduce<FROM: Any + Clone + for<'de> Deserialize<'de> + Serialize + Sync + Send, F: Fn(&FROM, &FROM) -> FROM + Sync + Send + 'static>(f: Arc<F>, data: &Vec<u8>) -> Vec<u8> {
    let data = bincode::deserialize::<Vec<FROM>>(data).unwrap();
    let result = data.par_iter().cloned()
    .reduce_with(|a, b| f(&a,&b))
    .unwrap();
    bincode::serialize(&result).unwrap()
}


#[macro_export]
macro_rules! add_function_reduce {
    ($context:ident, $function:expr) => {
        let f = Arc::new($function);
        $context.functions_reduce_map_rayon.insert(stringify!($function).to_string(), Arc::new(move |data| wrapper_rayon_reduce(Arc::clone(&f),data)));
    };
}

#[derive(Clone)]
pub struct Context {
    pub functions_map: HashMap<String, Arc<dyn Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub functions_map_rayon: HashMap<String, Arc<dyn Fn(&Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub functions_reduce_map: HashMap<String, Arc<dyn Fn(Vec<u8>,Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub functions_reduce_map_rayon: HashMap<String, Arc<dyn Fn(&Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub clients: Vec<MapReduceClient<Channel>>,
}

impl Context {
    pub fn new() -> Self {
        Context {
            functions_map: HashMap::new(),
            functions_map_rayon: HashMap::new(),
            functions_reduce_map: HashMap::new(),
            functions_reduce_map_rayon: HashMap::new(),
            clients: Vec::new(),
        }
    }
    
    pub fn map_function_rayon_serialized(&self, name: &str, data: &Vec<u8>) -> Option<Vec<u8>> {
        let function = self.functions_map_rayon.get(name);
        match function {
            Some(f) => {
                Some(f(data))
            },
            None => {
                println!("Function {} not found",name);
                None
            }
            
        }
    }

    pub fn reduce_function_rayon_serialized(&self, name: &str, data: &Vec<u8>) -> Option<Vec<u8>> {
        let function = self.functions_reduce_map_rayon.get(name);
        match function {
            Some(f) => {
                Some(f(data))
            },
            None => {
                println!("Worker: Function {} not found, length of the name: {}",name, name.len());
                None
            }
        }
    }

    async fn map_function_distributed<FROM: Any + Serialize + Sync , TO: for<'de> Deserialize<'de> + Send + Clone >(&mut self, name: &str, data: &Vec<FROM>) -> Result<Vec<TO>, Box<dyn std::error::Error>> {
        // with support for multiple workers
        let clinet_len = self.clients.len();
        let mut results = Vec::new();
        let mut futures = Vec::new();
        // use split_at_mut to split the data into chunks and split clients into chunks then zip them together and for each chunk create a future
        for (data_chunk, client_chunk) in data.chunks(data.len()/clinet_len).zip(self.clients.split_at_mut(clinet_len).0) {
            let serialized = bincode::serialize(&data_chunk).unwrap();
            let request_map = tonic::Request::new(MapRequest {
                function:  name.into(),
                data:  serialized.into(),
            });
            futures.push(client_chunk.map_chunk(request_map));
        }
        // wait for all futures to finish and collect the results
        for future in futures {
            let response = future.await?;
            results.push(bincode::deserialize::<Vec<TO>>(&response.into_inner().data).unwrap());
        }
        // flatten the results
        Ok(results.into_iter().flatten().collect())


    }
    
    // this function exists only because we want to make sure function is the right type and you should always use macro
    pub async fn call_map_function_distributed<FROM: Any + Serialize + Send + Sync, TO: for<'de> Deserialize<'de>  + Send + Sync + Clone , F: Fn(&FROM) -> TO + 'static>(
        &mut self,
        _: F, 
        func_name: &str, 
        data: &Vec<FROM>
    ) -> Result<Vec<TO>, Box<dyn std::error::Error>> {

        if self.clients.len() == 0 {
            println!("Client is none");
            Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Client is none")))
        }else{
            self.map_function_distributed::<FROM, TO>(func_name, data).await
        }
       
    }

    pub async fn reduce_function_distributed<FROM: Any + Serialize + Sync + for<'de> Deserialize<'de> + Send >(&mut self, name: &str, data: &Vec<FROM>) -> Result<FROM, Box<dyn std::error::Error>> {
        let clinet_len = self.clients.len();
        let mut results = Vec::new();
        let mut futures = Vec::new();
        // use split_at_mut to split the data into chunks and split clients into chunks then zip them together and for each chunk create a future
        for (data_chunk, client_chunk) in data.chunks(data.len()/clinet_len).zip(self.clients.split_at_mut(clinet_len).0) {
            let serialized = bincode::serialize(&data_chunk).unwrap();
            let request_map = tonic::Request::new(MapRequest {
                function:  name.into(),
                data:  serialized.into(),
            });
            futures.push(client_chunk.reduce_chunk(request_map));
        }
        // wait for all futures to finish and collect the results
        for future in futures {
            let response = future.await?;
            results.push(bincode::deserialize::<FROM>(&response.into_inner().data).unwrap());
        }
        // reduce the results 
        // effectively just run single worker reduce function on the results
        
        let final_reduction_request = tonic::Request::new(MapRequest {
            function:  name.into(),
            data:  bincode::serialize(&results).unwrap().into(),
        });

        let response = self.clients[0].reduce_chunk(final_reduction_request).await;
        match response {
            Ok(x) => {
                Ok(bincode::deserialize::<FROM>(&x.into_inner().data).unwrap())
            },
            Err(e) => {
                println!("Error: {}",e);
                Err(Box::new(e))
            }
        }
        

    }
    
    // this function exists only because we want to make sure function is the right type and you should always use macro
    pub async fn call_reduce_function_distributed<FROM: Any + Serialize + Send + Sync + for<'de> Deserialize<'de>, F: Fn(&FROM,&FROM) -> FROM + 'static>(
        &mut self,
        _: F, 
        func_name: &str, 
        data: &Vec<FROM>
    ) -> Result<FROM, Box<dyn std::error::Error>> {
        //check if function exists
        let function = self.functions_reduce_map_rayon.get(func_name);
        if function.is_none() {
            println!("Function {} not found",func_name);
            return Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Function not found")))
        }
        
        if self.clients.len() == 0 {
            println!("Client is none");
            Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Client is none")))
        }else{
            Context::reduce_function_distributed::<FROM>(self,func_name, data).await
        }
        
       
    }

    pub async fn map_reduce_function_distributed<
        FROM: Any + Serialize + for<'de> Deserialize<'de> + Sync + Send + Clone,
        TO: Any + Serialize + for<'da> Deserialize<'da> + Sync + Send + Clone >
        (&mut self, mapping_function_name: &str, reducing_function_name: &str, data: &Vec<FROM>) -> Result<TO, Box<dyn std::error::Error>> {
        

        let clinet_len = self.clients.len();
        let mut results = Vec::new();
        let mut futures = Vec::new();
        // use split_at_mut to split the data into chunks and split clients into chunks then zip them together and for each chunk create a future
        for (data_chunk, client_chunk) in data.chunks(data.len()/clinet_len).zip(self.clients.split_at_mut(clinet_len).0) {
            let serialized = bincode::serialize(&data_chunk).unwrap();
            let request_map_reduce: tonic::Request<MapReduceRequest> = tonic::Request::new(MapReduceRequest {
                function_map:  mapping_function_name.into(),
                function_reduce: reducing_function_name.into(),
                data:  serialized.into(),
            });
            futures.push(client_chunk.map_reduce_chunk(request_map_reduce));
        }
        // wait for all futures to finish and collect the results
        for future in futures {
            let response = future.await?;
            results.push(bincode::deserialize::<FROM>(&response.into_inner().data).unwrap());
        }
        // reduce the results 
        // effectively just run single worker reduce function on the results
        
        let final_reduction_request = tonic::Request::new(MapRequest {
            function:  reducing_function_name.into(),
            data:  bincode::serialize(&results).unwrap().into(),
        });

        let response = self.clients[0].reduce_chunk(final_reduction_request).await;
        match response {
            Ok(x) => {
                Ok(bincode::deserialize::<TO>(&x.into_inner().data).unwrap())
            },
            Err(e) => {
                println!("Error: {}",e);
                Err(Box::new(e))
            }
        }
        

    }

    pub async fn call_map_reduce_functions_distributed<
        FROM: Any +  for<'de> Deserialize<'de> + Serialize + Send + Sync + Clone,
        TO: Any + for<'da> Deserialize<'da> + Serialize + Send + Sync + Clone,
        FMap: Fn(&FROM) -> TO + 'static ,
        FReduce: Fn(&TO,&TO) -> TO + 'static > (
        &mut self,
        _: FMap,
        _: FReduce, 
        map_func_name: &str, 
        reduce_func_name: &str,
        data: &Vec<FROM>
    ) -> Result<TO, Box<dyn std::error::Error>> {
        
        let function_map = self.functions_map_rayon.get(map_func_name);
        let function_reduce = self.functions_reduce_map_rayon.get(reduce_func_name);
        if function_map.is_none() {
            println!("Mapping function {} not found",map_func_name);
            return Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Mapping function not found")))
        }

        if function_reduce.is_none() {
            println!("Reduce function {} not found",reduce_func_name);
            return Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Reduce function not found")))
        }

        if self.clients.len() == 0 {
            println!("Client is none");
            Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Client is none")))
        }else{
            let mapped_data = self.map_function_distributed::<FROM, TO>(map_func_name, data).await?;
            let reduced_data = self.reduce_function_distributed::<TO>(reduce_func_name, &mapped_data).await?;
            Ok(reduced_data)
        }
       
    }
    

    pub async fn update_workers_pool(&mut self)-> Result<(), Box<dyn std::error::Error>>{
        let workers_url_filename = std::env::var("WORKERS_URL").unwrap_or("/workers_urls".to_string());
        let workers_url = std::fs::read_to_string(workers_url_filename).expect("Something went wrong reading the file");
        let workers_url = workers_url.trim_end_matches('\n');
        let workers_url_vec: Vec<String> = workers_url.split(",").map(|s| s.to_string()).collect();
        println!("WORKERS_URL={:?}", workers_url_vec);
    
        for worker_url in workers_url_vec {
            let worker_url = format!("http://{}", worker_url);
            println!("Connecting to worker: {}", worker_url);
            match timeout(Duration::from_secs(3), MapReduceClient::connect(worker_url)).await {
                Ok(Ok(client)) => self.clients.push(client),
                Ok(Err(e)) => println!("Failed to connect to worker: {}", e),
                Err(_) => println!("Connection to worker timed out"),
            }
        }
    
        // if len of clients is 0 then exit
        if self.clients.len() == 0 {
            println!("No workers found");
            return Ok(());
        }

        println!("Number of workers: {}", self.clients.len());

        Ok(())

    }

}

#[macro_export]
macro_rules! map_distributed {
    ($context:ident, $func:ident, $data:ident) => {
        ($context).call_map_function_distributed($func, stringify!($func), &$data).await?
    };
}

#[macro_export]
macro_rules! reduce_distributed {
    ($context:ident, $func:ident, $data:ident) => {
        ($context).call_reduce_function_distributed($func, stringify!($func), &$data).await?
    };
}

#[macro_export]
macro_rules! map_reduce_distributed {
    ($context:ident, $func_map:ident, $func_reduce:ident, $data:ident) => {
        ($context).call_map_reduce_functions_distributed($func_map, $func_reduce, stringify!($func_map), stringify!($func_reduce), &$data).await?
    };
}

pub async fn worker_master_split(context: &Context) -> Result<(), Box<dyn std::error::Error>>{
    match env::var("ROLE") {
        Ok(val) => {
            let val = val.trim();
            println!("I'm {} it's {}", val , chrono::Local::now().naive_local());
            match val {
                "master" => {
                    ()
                },
                "worker" => {
                    worker_service::do_worker(context).await?;
                    // kill worker so it won't start doing master's code
                    std::process::exit(0);
                },
                _ => {
                    println!("Incorrect ROLE environment variable. Set to either 'master' or 'worker'");
                }
            }
        },
        Err(_) => {
            println!("You didn't set ROLE environment variable. Set to either 'master' or 'worker'");
        },
    }
    Ok(())
}
