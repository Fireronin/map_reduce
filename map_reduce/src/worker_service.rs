use tonic::{transport::Server, Request, Response, Status};
use tokio::signal;
use map_reduce::map_reduce_server::*;
use map_reduce::*;
use crate::map_reduce_utils::*;
use tokio::sync::Notify;


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



pub async fn do_worker(context: &Context) -> Result<(), Box<dyn std::error::Error>> {
    //let addr = "[::1]:50052".parse().unwrap();
    // grab ip from env variable
    let addr = std::env::var("WORKER_URL").unwrap_or("[::1]:9000".to_string()).parse().unwrap();

    let greeter = MapReduceWorker::new(&context);

    let shutdown = Notify::new();

    let ctrl_c = tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
        shutdown.notify_one();
    });

    println!("Listening on {}", addr);

    let server = Server::builder()
        .add_service(MapReduceServer::new(greeter))
        .serve(addr);

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