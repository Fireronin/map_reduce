use serde::{Serialize, Deserialize};
use bincode::serialize;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::any::Any;
use rayon::prelude::*;
use std::sync::Arc;

// Define the struct
#[derive(Serialize, Deserialize, Debug, Clone,Copy,Default)]
pub struct KeyValue {
    pub key: i32,
    pub value: i64,
}

pub fn wrapper<FROM: Any + for<'de> Deserialize<'de>, TO: Serialize, F: Fn(&FROM) -> TO + 'static>(f: Arc<F>,data: Vec<u8>) -> Vec<u8> {
    let data = bincode::deserialize::<FROM>(&data).unwrap();
    let result = f(&data);
    bincode::serialize(&result).unwrap()
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
        //$context.functions_map.insert(stringify!($function).to_string(), Arc::new(move |data| wrapper(Arc::clone(f),data)));
        $context.functions_map_rayon.insert(stringify!($function).to_string(), Arc::new(move |data| wrapper_rayon(Arc::clone(&f),&data)));
    };
}





pub fn wrapper_reduce<FROM: Any + for<'de> Deserialize<'de> + Serialize, F: Fn(FROM, FROM) -> FROM + 'static>(f: Arc<F>,data_a: Vec<u8>,data_b: Vec<u8>) -> Vec<u8> {
    let data_a = bincode::deserialize::<FROM>(&data_a).unwrap();
    let data_b = bincode::deserialize::<FROM>(&data_b).unwrap();
    let result = f(data_a,data_b);
    bincode::serialize(&result).unwrap()
}

#[macro_export]
macro_rules! add_function_reduce {
    ($context:ident, $function:expr) => {
        let f = Arc::new($function);
        $context.functions_reduce_map.insert(stringify!($function).to_string(), Arc::new(move |data_a,data_b| wrapper_reduce(Arc::clone(&f),data_a,data_b)));
    };
}


pub struct Context {
    pub functions_map: HashMap<String, Arc<dyn Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub functions_map_rayon: HashMap<String, Arc<dyn Fn(&Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
    pub functions_reduce_map: HashMap<String, Arc<dyn Fn(Vec<u8>,Vec<u8>) -> Vec<u8> + Send + Sync + 'static>>,
}

impl Context {
    pub fn new() -> Self {
        Context {
            functions_map: HashMap::new(),
            functions_map_rayon: HashMap::new(),
            functions_reduce_map: HashMap::new(),
        }
    }

    pub fn call_function<FROM: Any + Serialize, TO: for<'de> Deserialize<'de> >(&self, name: &str, data: FROM) -> TO {
        let function = self.functions_map.get(name).unwrap();
        let serialized_data = bincode::serialize(&data).unwrap();
        let result = function(serialized_data);
        bincode::deserialize::<TO>(&result).unwrap()
    }

    pub fn map_function<FROM: Any + Serialize, TO: for<'de> Deserialize<'de> >(&self, name: &str, data: Vec<FROM>) -> Vec<TO> {
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

    pub fn map_function_rayon<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send >(&self, name: &str, data: Vec<FROM>) -> Vec<TO> {
        let function = self.functions_map.get(name).unwrap();
        let result: Vec<_> = data.par_iter()
        .map(|item| {
            let serialized_item = bincode::serialize(&item).unwrap();
            let processed_item = function(serialized_item.clone());
            bincode::deserialize::<TO>(&processed_item).unwrap()
        })
        .collect();
        result
    }

    pub fn map_function_rayon_serialized(&self, name: &str, data: &Vec<u8>) -> Vec<u8> {
        let function = self.functions_map_rayon.get(name).unwrap();
        function(data)
    }

    
    pub fn reduce_function<FROM: Any + Serialize + for<'de> Deserialize<'de> >(&self, name: &str, data: Vec<FROM>) -> FROM {
        let function = self.functions_reduce_map.get(name).unwrap();
        // get first item
        let mut result = bincode::serialize(&data[0]).unwrap();
        for item in data.iter().skip(1) {
            let serialized_data = bincode::serialize(&item).unwrap();
            result = function(result,serialized_data);
        }
        bincode::deserialize::<FROM>(&result).unwrap()

    }

    pub fn reduce_function_rayon<FROM: Any + Serialize + Sync + for<'de> Deserialize<'de> + Send >(&self, name: &str, data: Vec<FROM>) -> FROM {
        let function = self.functions_reduce_map.get(name).unwrap();
        // let serialized_data = data.par_iter().map(|item| bincode::serialize(&item).unwrap()).collect::<Vec<Vec<u8>>>();
        // let result = serialized_data.into_par_iter().reduce_with(|a,b| function(a,b)).unwrap();
        let result = data.par_iter()
        .map(|item| bincode::serialize(&item).unwrap())
        .reduce_with(|a, b| function(a, b))
        .unwrap();
        bincode::deserialize::<FROM>(&result).unwrap()

    }
}



// pub fn call_map_function_rayon<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send, F: Fn(FROM) -> TO + Sync + Send + 'static>(
//     context: &Context, 
//     _: F, 
//     func_name: &str, 
//     data: Vec<FROM>
// ) -> Vec<TO> {
//     context.map_function_rayon::<FROM, TO>(func_name, data)
// }

// #[macro_export]
// macro_rules! map_function_rayon {
//     ($context:ident, $func:ident, $data:ident) => {
//         call_map_function_rayon(&$context, $func, stringify!($func), $data)
//     };
// }


// pub fn call_map_function_rayon_serialized<FROM: Any + Serialize + Sync, TO: for<'de> Deserialize<'de> + Send, F: Fn(FROM) -> TO + Sync + Send + 'static>(
//     context: &Context, 
//     _: F, 
//     func_name: &str, 
//     data: &Vec<u8>
// ) -> Vec<u8> {
//     context.map_function_rayon_serialized::<FROM, TO>(func_name, data)
// }

// #[macro_export]
// macro_rules! map_function_rayon_serialized {
//     ($context:ident, $func:ident, $data:ident) => {
//         call_map_function_rayon_serialized(&$context, $func, stringify!($func), &$data)
//     };
// }

// same for normal map
pub fn call_map_function<FROM: Any + Serialize, TO: for<'de> Deserialize<'de> , F: Fn(FROM) -> TO + 'static>(
    context: &Context, 
    _: F, 
    func_name: &str, 
    data: Vec<FROM>
) -> Vec<TO> {
    context.map_function::<FROM, TO>(func_name, data)
}

#[macro_export]
macro_rules! map_function {
    ($context:ident, $func:ident, $data:ident) => {
        call_map_function(&$context, $func, stringify!($func), $data)
    };
}

pub fn call_reduce_function_rayon<FROM: Any + Serialize + Sync +Send + for<'de> Deserialize<'de>, F: Fn(FROM, FROM) -> FROM + Sync + Send + 'static>(
    context: &Context, 
    _: F, 
    func_name: &str, 
    data: Vec<FROM>
) -> FROM {
    context.reduce_function_rayon::<FROM>(func_name, data)
}

#[macro_export]
macro_rules! reduce_function_rayon {
    ($context:ident, $func:ident, $data:ident) => {
        call_reduce_function_rayon(&$context, $func, stringify!($func), $data)
    };
}

pub fn multiply_by_2(x: &KeyValue) -> KeyValue {
    KeyValue {
        key: x.key,
        value: x.value * 2,
    }
}

pub fn sum(x: KeyValue, y: KeyValue) -> KeyValue {
    KeyValue {
        key: x.key,
        value: x.value + y.value,
    }
}

// pub fn map_reduce_example() {
//     // Create a vector of KeyValue structs
//     let data = vec![
//         KeyValue { key: 1, value: 10 },
//         KeyValue { key: 2, value: 20 },
//         KeyValue { key: 3, value: 30 },
//     ];

//     let mut data_long = Vec::new();
//     for i in 0..100000 {
//         data_long.push(KeyValue { key: i, value: i as i64 });
//     }

//     let mut context = Context::new();
//     add_function!(context, multiply_by_2);
//     add_function_reduce!(context, sum);

//     let start = std::time::Instant::now();

//     //let result = map_function!(context,multiply_by_2,data_long);
//     let result = map_function_rayon!(context,multiply_by_2,data_long);
    
//     let summed = reduce_function_rayon!(context,sum,result);

//     let elapsed = start.elapsed();
//     println!("Elapsed: {:.2?}", elapsed);

//     println!("{:?}", summed);
//     // for item in result {
//     //     println!("{:?}", item);
//     // }


// }
