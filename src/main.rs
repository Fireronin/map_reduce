use std::sync::Arc;
use serde::{Serialize, Deserialize};
use map_reduce::map_reduce_client::MapReduceClient;
use crate::map_reduce_utils::*;


#[macro_use]
mod map_reduce_utils;
pub mod worker_service;


// Example data structure
#[derive(Serialize, Deserialize, Debug, Clone,Default)]
pub struct KeyValue {
    pub key: i32,
    pub value: i64,
}

// Example map and reduce functions

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

// $env:ROLE="master"; cargo run
// $env:ROLE="worker"; cargo run
// set ROLE=master && cargo run
// set ROLE=worker && cargo run

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // here we create a context that will be shared between master and worker
    let mut context = Context::new();
    // we register functions
    add_function!(context, multiply_by_2);
    add_function_reduce!(context, sum);

    worker_master_split(&context).await?; //after this line only master will run

    /* 
    // Create the client outside of the context
    let mut my_client = MapReduceClient::connect("http://[::1]:50052").await?;

    // sample data
    let data_vec = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
    ];

    // Use the macros with the provided client
    let result = map_distributed!(context, multiply_by_2, data_vec, &mut my_client);
    println!("Map RESPONSE={:?}", result);

    let result = reduce_distributed!(context, sum, result, &mut my_client);
    println!("Reduce RESPONSE={:?}", result);
    */
    Ok(())
}

