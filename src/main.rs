#![allow(unused_imports)]

use std::io::Write;
use std::net::TcpListener;
use serde::Serialize;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let message = Message { size: 3, header: 7 };
                _stream.write(&message.into_bytes().as_slice());
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Serialize)]
pub struct Message {
    pub size: i32,
    pub header: i32
}

impl Message {
    pub fn into_bytes(self) -> Vec<u8> {
        let mut buffer = Vec::<u8>::with_capacity(8);
        buffer.extend_from_slice(&self.size.to_be_bytes());
        buffer.extend_from_slice(&self.header.to_be_bytes());
        buffer
    }
}