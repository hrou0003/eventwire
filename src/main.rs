#![allow(unused_imports)]

use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::net::TcpListener;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:9092").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                let mut size_buffer = [0; 4];
                if _stream.read_exact(&mut size_buffer).is_err() {
                    println!("error: failed to read message size");
                    continue; // Skip to the next connection
                }
                let message_size = i32::from_be_bytes(size_buffer);

                let mut message_buffer = vec![0; message_size as usize];
                if _stream.read_exact(&mut message_buffer).is_err() {
                    println!("error: failed to read message body");
                    continue;
                }

                match bincode::deserialize::<Message>(&message_buffer) {
                    Ok(message) => {
                        println!("Received message: {:?}", message);

                        let response_header = Header {
                            correlation_id: message.header.correlation_id,
                            request_api_key: message.header.request_api_key,
                            request_api_version: message.header.request_api_version,
                            client_id: None,
                            tag_buffer: None,
                        };

                        let response_message = Message {
                            size: std::mem::size_of::<Header>() as i32,
                            header: response_header,
                        };

                        if let Ok(response_bytes) = bincode::serialize(&response_message) {
                            let response_len = response_bytes.len() as i32;
                            if _stream.write_all(&response_len.to_be_bytes()).is_ok() {
                                if _stream.write_all(&response_bytes).is_ok() {
                                    println!("Wrote response message");
                                } else {
                                    println!("Error writing response message");
                                }
                            } else {
                                println!("Error writing response size");
                            }
                        } else {
                            println!("error: failed to serialize response message");
                        }
                    }
                    Err(e) => {
                        println!("error: failed to deserialize message: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub size: i32,
    pub header: Header,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Header {
    pub request_api_key: i16,
    pub request_api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub tag_buffer: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {}
