use serde::{Serialize, Deserialize};
use bincode::serialize;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::any::Any;
use rayon::prelude::*;
use std::sync::Arc;

// Define the struct
#[derive(Serialize, Deserialize, Debug, Clone,Copy)]
struct KeyValue {
    key: i32,
    value: i32,
}

fn wrapper<FROM: Any + for<'de> Deserialize<'de>, TO: Serialize, F: Fn(FROM) -> TO + 'static>(f: Arc<F>,data: Vec<u8>) -> Vec<u8> {
    let data = bincode::deserialize::<FROM>(&data).unwrap();
    let result = f(data);
    bincode::serialize(&result).unwrap()
}

macro_rules! add_function {
    ($context:ident, $name:ident, $function:expr) => {
        let f = Arc::new($function);
        $context.functions_map.insert(stringify!($name).to_string(), Arc::new(move |data| wrapper(Arc::clone(&f),data)));
    };
}

struct Context {
    functions_map: HashMap<String, Arc<dyn Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
}

impl Context {
    fn new() -> Self {
        Context {
            functions_map: HashMap::new(),
        }
    }

    fn call_function<FROM: Any + Serialize, TO: for<'de> Deserialize<'de> >(&self, name: &str, data: FROM) -> TO {
        let function = self.functions_map.get(name).unwrap();
        let serialized_data = bincode::serialize(&data).unwrap();
        let result = function(serialized_data);
        bincode::deserialize::<TO>(&result).unwrap()
    }

    fn map_function<FROM: Any + Serialize, TO: for<'de> Deserialize<'de> >(&self, name: &str, data: Vec<FROM>) -> Vec<TO> {
        let function = self.functions_map.get(name).unwrap();
        let mut result = Vec::new();
        for item in data {
            let serialized_data = bincode::serialize(&item).unwrap();
            let result_item = function(serialized_data);
            let result_item = bincode::deserialize::<TO>(&result_item).unwrap();
            result.push(result_item);
        }
        result

    }

    fn map_function_rayon<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send >(&self, name: &str, data: Vec<FROM>) -> Vec<TO> {
        let function = self.functions_map.get(name).unwrap();
        let serialized_data = data.par_iter().map(|item| bincode::serialize(&item).unwrap()).collect::<Vec<Vec<u8>>>();
        let result = serialized_data.par_iter().map(|item| function(item.clone())).collect::<Vec<Vec<u8>>>();
        let mut deserialized_result = Vec::new();
        for item in result {
            let item = bincode::deserialize::<TO>(&item).unwrap();
            deserialized_result.push(item);
        }
        deserialized_result
    }

    

    

}



fn call_map_function_rayon<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send, F: Fn(FROM) -> TO + Sync + Send + 'static>(
    context: &Context, 
    _: F, 
    func_name: &str, 
    data: Vec<FROM>
) -> Vec<TO> {
    context.map_function_rayon::<FROM, TO>(func_name, data)
}

macro_rules! map_function_rayon {
    ($context:ident, $func:ident, $data:ident) => {
        call_map_function_rayon(&$context, $func, stringify!($func), $data)
    };
}

fn multiply_by_2(x: KeyValue) -> KeyValue {
    KeyValue {
        key: x.key,
        value: x.value * 2,
    }
}

fn main() {
    // Create a vector of KeyValue structs
    let data = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
        KeyValue { key: 3, value: 30 },
    ];

    let mut data_long = Vec::new();
    for i in 0..1000000 {
        data_long.push(KeyValue { key: i, value: i });
    }

    let mut context = Context::new();
    add_function!(context, map, multiply_by_2);
    
    
    let start = std::time::Instant::now();

    let result = map_function_rayon!(context,multiply_by_2,data);
    //let result =  context.map_function_rayon::<KeyValue,KeyValue>("map",data_long);

    let elapsed = start.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    // for item in result {
    //     println!("{:?}", item);
    // }


}