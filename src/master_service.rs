use hello_world::*;
use hello_world::greeter_client::GreeterClient;
use crate::map_reduce_utils::*;

pub mod hello_world {
    tonic::include_proto!("hello_world");
}

//#[tokio::main]
pub async fn do_master() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = GreeterClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(HelloRequest {
        message: "Tonic".into(),
    });

    let response = client.say_hello(request).await?;

    println!("RESPONSE={:?}", response);
    let data_vec = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
    ];

    let serialized = bincode::serialize(&data_vec).unwrap();

    let request_map = tonic::Request::new(MapRequest {
        function:  stringify!(multiply_by_2).into(),
        data:  serialized.into(),
    });

    let response = client.map_chunk(request_map).await?;

    let response =bincode::deserialize::<Vec<KeyValue>>(&response.into_inner().data).unwrap();

    println!("RESPONSE={:?}", response);

    Ok(())
}