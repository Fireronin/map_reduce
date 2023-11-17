use std::os::windows;

use tonic::{transport::Server, Request, Response, Status};

use hello_world::greeter_server::*;
use hello_world::*;
use crate::map_reduce_utils::*;

pub mod hello_world {
    tonic::include_proto!("hello_world");
}


pub struct MyGreeter {
    context : Context,
}

impl MyGreeter {
    fn new(context: &Context) -> Self {
        MyGreeter {
            context: context.clone(),
        }
    }
}

#[tonic::async_trait]
impl Greeter for MyGreeter {

    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = hello_world::HelloReply {
            message: format!("Hello {}!", request.into_inner().message),
        };
        Ok(Response::new(reply))
    }

    async fn map_chunk(
        &self,
        request: Request<MapRequest>,
    ) -> Result<Response<MapReply>, Status> {

        let inner_request = request.into_inner();
        let function = inner_request.function.as_str();
        let data = &inner_request.data;
        let output = self.context.map_function_rayon_serialized(function,data);
        
        let reply = hello_world::MapReply {
            data: output,
        };

        Ok(Response::new(reply))
    }
}

// #[tokio::main]
pub async fn do_worker(context: &Context) -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let greeter = MyGreeter::new(&context);

    println!("GreeterServer listening on {}", addr);
    
    Server::builder()
        .add_service(GreeterServer::new(greeter))
        .serve(addr)
        .await?;

    Ok(())
}