use serde::{Serialize, Deserialize};
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::collections::HashMap;
use std::boxed::Box;
use std::any::Any;

// Define the struct
#[derive(Serialize, Deserialize, Debug, Clone)]
struct KeyValue {
    key: i32,
    value: i32,
}




trait FnBox {
    fn call_box(&self, arg: &dyn Any) -> Box<dyn Any>;
}

impl<F: Fn(&dyn Any) -> Box<dyn Any> > FnBox for F {
    fn call_box(&self, arg: &dyn Any) -> Box<dyn Any> {
        (*self)(arg)
    }
}

macro_rules! map {
    ($func:expr, $vec:expr) => {{
        let mut result = Vec::new();
        for item in $vec {
            result.push($func(item.clone()));
        }
        result
    }};
}

macro_rules! add_function {
    ($map:expr, $func:expr) => {
        $map.insert(stringify!($func).to_string(), Box::new(move |arg: &dyn Any| $func(arg.downcast_ref().unwrap())));
    };
}

fn multiply_by_two(x: &KeyValue) -> Box<dyn Any> {
    Box::new(KeyValue { key: x.key, value: x.value * 2 })
}


#[derive(Default)]
struct Context {
    functions_map: HashMap<String, Box<dyn FnBox>>,
}

impl Context {
    fn new() -> Context {
        Context {
            functions_map: HashMap::new(),
        }
    }

    fn add_function(&mut self, name: &str, func: Box<dyn FnBox>) {
        self.functions_map.insert(name.to_string(), func);
    }

    fn get_function(&self, name: &str) -> Option<&Box<dyn FnBox>> {
        self.functions_map.get(name)
    }

    fn run_function(&self, name: &str, arg: &dyn Any) -> Option<Box<dyn Any>> {
        let func = self.get_function(name)?;
        Some(func.call_box(arg))
    }

    fn map(&self, name: &str, vec: Vec<Box<dyn Any>>) -> Option<Vec<Box<dyn Any>>> {
        let func = self.get_function(name)?;
        let mut result = Vec::new();
        for item in vec {
            result.push(func.call_box(item.as_ref()));
        }
        Some(result)
    }
}

fn to_any_vec(data: Vec<KeyValue>) -> Vec<Box<dyn Any>> {
    data.into_iter().map(|x| Box::new(x) as Box<dyn Any>).collect()
}

fn main() {
    // Create a vector of KeyValue structs
    let data = vec![
        KeyValue { key: 1, value: 10 },
        KeyValue { key: 2, value: 20 },
        KeyValue { key: 3, value: 30 },
    ];

    let mut context = Context::new();

    add_function!(context.functions_map, multiply_by_two); 
    

    let result = context.map("multiply_by_two", to_any_vec(data)).unwrap();

    for item in result {
        let item = item.downcast_ref::<KeyValue>().unwrap();
        println!("{} {}", item.key, item.value);
    }

}