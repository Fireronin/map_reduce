use serde::{Serialize, Deserialize};
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
    pub client: Option<MapReduceClient<Channel>>,
}

impl Context {
    pub fn new() -> Self {
        Context {
            functions_map: HashMap::new(),
            functions_map_rayon: HashMap::new(),
            functions_reduce_map: HashMap::new(),
            functions_reduce_map_rayon: HashMap::new(),
            client: None,
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

    async fn map_function_distributed<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send >(&mut self, name: &str, data: &Vec<FROM>) -> Result<Vec<TO>, Box<dyn std::error::Error>> {
        let serialized = bincode::serialize(&data).unwrap();
        let request_map = tonic::Request::new(MapRequest {
            function:  name.into(),
            data:  serialized.into(),
        });
        let response = self.client.as_mut().unwrap().map_chunk(request_map).await?;
        Ok(bincode::deserialize::<Vec<TO>>(&response.into_inner().data).unwrap())
    }
    
    // this function exists only because we want to make sure function is the right type and you should always use macro
    pub async fn call_map_function_distributed<FROM: Any + Serialize + Send + Sync, TO: for<'de> Deserialize<'de>  + Send + Sync , F: Fn(&FROM) -> TO + 'static>(
        &mut self,
        _: F, 
        func_name: &str, 
        data: &Vec<FROM>
    ) -> Result<Vec<TO>, Box<dyn std::error::Error>> {
        if self.client.is_none() {
            println!("Client is none");
            Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Client is none")))
        }else{
            self.map_function_distributed::<FROM, TO>(func_name, data).await
        }
       
    }

    pub async fn reduce_function_distributed<FROM: Any + Serialize + Sync + for<'de> Deserialize<'de> + Send >(client: &mut MapReduceClient<Channel>, name: &str, data: &Vec<FROM>) -> Result<FROM, Box<dyn std::error::Error>> {
        let serialized = bincode::serialize(&data).unwrap();
        let request_map = tonic::Request::new(MapRequest {
            function:  name.into(),
            data:  serialized.into(),
        });
        let response = client.reduce_chunk(request_map).await;
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
        
        if self.client.is_none() {
            println!("Client is none");
            Err(Box::new(tonic::Status::new(tonic::Code::Internal, "Client is none")))
        }else{
            Context::reduce_function_distributed::<FROM>(self.client.as_mut().unwrap(),func_name, data).await
        }
        
       
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

pub async fn worker_master_split(context: &Context) -> Result<(), Box<dyn std::error::Error>>{
    match env::var("ROLE") {
        Ok(val) => {
            let val = val.trim();
            println!("I'm {}", val);
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
