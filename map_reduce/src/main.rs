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

    // read env variable WORKERS_URL
    let workers_url_filename = std::env::var("WORKERS_URL").unwrap_or("/workers_urls".to_string());
    // read file WORKERS_URL
    let workers_url = std::fs::read_to_string(workers_url_filename).expect("Something went wrong reading the file");
    
    // remove \n from the end of the string
    let workers_url = workers_url.trim_end_matches('\n');
    // add http:// to the beginning of the string
    let workers_url = format!("http://{}", workers_url);
    
    // split , and create a vector of workers url
    let workers_url_vec: Vec<String> = workers_url.split(",").map(|s| s.to_string()).collect();
    //context.client = Some(MapReduceClient::connect("http://[::1]:50052").await?);
    println!("WORKERS_URL={:?}", workers_url_vec);
    context.client = Some(MapReduceClient::connect(workers_url_vec[0].clone()).await?);

    // sample data
    let data_vec = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
    ];

    let result = map_distributed!(context, multiply_by_2, data_vec);
    println!("Map RESPONSE={:?}", result);

    let result = reduce_distributed!(context,sum, result);
    println!("Reduce RESPONSE={:?}", result);

    Ok(())
}
