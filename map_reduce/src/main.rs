use std::{sync::Arc};
use serde::{Serialize, Deserialize};
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

    context.update_workers_pool().await?; //update workers pool 

    // sample data with 10 elements
    let data_vec: Vec<KeyValue> = (1..10).map(|x| KeyValue { key: x, value: x as i64 }).collect();

    let result = map_distributed!(context, multiply_by_2, data_vec);
    println!("Map RESPONSE={:?}", result);

    let result = reduce_distributed!(context,sum, result);
    println!("Reduce RESPONSE={:?}", result);

    // now using single function

    let result = map_reduce_distributed!(context, multiply_by_2, sum, data_vec);
    println!("MapReduce RESPONSE={:?}", result);

    Ok(())
}


