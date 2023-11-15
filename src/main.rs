use tonic::transport::Server;
use std::env;
use std::sync::Arc;
// use master_service;
// use worker_service;
//use crate::map_reduce_utils::*;
#[macro_use]
mod map_reduce_utils;
pub mod worker_service;
pub mod master_service;
pub mod hello_world {
    tonic::include_proto!("hello_world");
}
use crate::map_reduce_utils::*;



#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut context = Context::new();
    add_function!(context, multiply_by_2);
    add_function_reduce!(context, sum);

    let role: Result<String, env::VarError> = env::var("ROLE");
    let role_string = role.unwrap_or("No data".to_string());
    println!("{} val {}",role_string,"master " ==role_string.as_str());
    match env::var("ROLE") {
        Ok(val) => {
            if "master " ==role_string.as_str() {
                master_service::do_master().await?;
            
                println!("RESPONSE={:?}", "OK");
            } else if "worker " ==role_string.as_str() {
                print!("WOrker");
                worker_service::do_worker(context).await?;
            } else {
                println!("AAAAIncorrect ROLE environment variable. Set to either 'master' or 'worker'");
            }
        },
        Err(_) => {
            println!("You didn't set ROLE environment variable. Set to either 'master' or 'worker'");
        },
    }

    Ok(())
}
