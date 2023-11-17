use tonic::transport::Server;
use std::env;
use std::sync::Arc;
use hello_world::*;
use hello_world::greeter_client::GreeterClient;
use serde::{Serialize, Deserialize};
use bincode::serialize;
use tonic::transport::Channel;
use std::default;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::collections::HashMap;

use std::any::Any;
use rayon::prelude::*;
use crate::map_reduce_utils::*;

#[macro_use]
mod map_reduce_utils;
pub mod worker_service;
pub mod master_service;

pub mod hello_world {
    tonic::include_proto!("hello_world");
}


async fn worker_master_split(context: &map_reduce_utils::Context) -> Result<(), Box<dyn std::error::Error>>{
    let role: Result<String, env::VarError> = env::var("ROLE");
    let role_string = role.unwrap_or("No data".to_string());
    println!("{} val {}",role_string,"master " ==role_string.as_str());
    match env::var("ROLE") {
        Ok(val) => {
            if "master " ==role_string.as_str() {
                //master_service::do_master().await?;
                
            } else if "worker " ==role_string.as_str() {
                print!("WOrker");
                worker_service::do_worker(context).await?
            } else {
                println!("Incorrect ROLE environment variable. Set to either 'master' or 'worker'");
                
            }
        },
        Err(_) => {
            println!("You didn't set ROLE environment variable. Set to either 'master' or 'worker'");
        },
    }
    Ok(())
}

pub async fn map_function_distributed<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send >(context: &Context, name: &str, data: &Vec<FROM>) -> Result<Vec<TO>, Box<dyn std::error::Error>> {
    let serialized = bincode::serialize(&data).unwrap();
    let request_map = tonic::Request::new(MapRequest {
        function:  name.into(),
        data:  serialized.into(),
    });
    let response = context.client.unwrap().map_chunk(request_map).await?;
    Ok(bincode::deserialize::<Vec<TO>>(&response.into_inner().data).unwrap())
}

pub async fn call_map_function_distributed<FROM: Any + Serialize + Send + Sync, TO: for<'de> Deserialize<'de>  + Send + Sync , F: Fn(FROM) -> TO + 'static>(
    context: &Context, 
    _: F, 
    func_name: &str, 
    data: &Vec<FROM>
) -> Result<Vec<TO>, Box<dyn std::error::Error>> {
    map_function_distributed::<FROM, TO>(context,func_name, data).await
}

// #[macro_export]
// macro_rules! map_function_distributed_macro {
//     ($context:ident, $func:ident, $data:ident) => {
//         call_map_function_distributed(&$context, $func, stringify!($func), $data).await?
//     };
// }

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = Context::new();
    add_function!(context, multiply_by_2);
    add_function_reduce!(context, sum);

    worker_master_split(&context);

    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    let data_vec = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
    ];
    let result = call_map_function_distributed(&context,multiply_by_2,"multiply_by_2",&data_vec).await?;


    println!("RESPONSE={:?}", result);


    Ok(())
}
