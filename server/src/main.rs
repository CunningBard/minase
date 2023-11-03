extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmp;

use std::io::Write;
use rmp::Deserializer;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use minase::db_core::database::{Database, Table};
use minase::db_core::query_error::QueryError;
use minase::db_core::values::{Column, Expr, Value};

// let mut buffer = vec![];
// result.serialize(&mut rmp::Serializer::new(&mut buffer))?;
//
//
// let res = Table::deserialize(&mut Deserializer::new(&buffer[..]))?;


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;



    // logger.info("Database Exiting".to_string(), "Execution has ended".to_string()).await;
    // logger.flush_buffer().await;

    loop {
        let (mut socket, _) = listener.accept().await?;


        tokio::spawn(async move {
            let mut size_buffer = [0; 4];
            let mut buffer = vec![];

            let mut logger = logger::Logger::new().await;

            logger.info("Database Starting".to_string(), "Execution has started".to_string()).await;

            let mut db = Database::new(
                &mut logger
            );

            db.add_table(
                vec![
                    Column::Int(vec![]),
                    Column::String(vec![]),
                ]
            ).await;

            for i in 0..100 {
                db.insert(
                    0,
                    vec![
                        Value::Int(i),
                        Value::String(format!("Person {}", i)),
                    ]
                ).await.expect("What the fuck?");
            }

            // println!("table: {:#?}", db);

            // flush stdout
            // std::io::stdout().flush();

            loop {
                // db.logger_flush().await;
                // Read data from the socket
                socket.read(&mut size_buffer).await.unwrap();

                let size = u32::from_le_bytes(size_buffer);
                buffer = vec![0; size as usize];

                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 { return; }

                let message = String::from_utf8(buffer[..n].to_owned()).unwrap();

                println!("Received: {}", message);

                let mut split = message.split_whitespace().collect::<Vec<&str>>();

                macro_rules! invalid_query {
                    () => {
                        let message = QueryError::InvalidQuery.to_string();
                        let size = message.len() as u32;
                        let mut size_buffer = size.to_be_bytes();
                        socket.write(&mut size_buffer).await.unwrap();
                        socket.write(message.as_bytes()).await.unwrap();
                        break;
                    }
                }


                if split.len() == 0 {
                    invalid_query!();
                }

                match split[0] {
                    "select" => {
                        /*
                        Two cases:
                            select [where <condition: expr>] from <table: number> <column: number>
                            select table <table: number>
                        */

                        match split[1] {
                            "table" => {
                                let table_id = split[2].parse::<usize>().unwrap();
                                // println!("Table ID: {}", table_id);
                                let table = db.get_table(table_id).await.unwrap();

                                // println!("Table: {:#?}", table);

                                let mut buffer = vec![];
                                table.serialize(&mut rmp::Serializer::new(&mut buffer)).unwrap();

                                let size = buffer.len() as u32;
                                let mut size_buffer = size.to_be_bytes();
                                socket.write(&mut size_buffer).await.unwrap();
                                socket.write(&mut buffer).await.unwrap();
                            }
                            _ => {
                                invalid_query!();
                            }
                        }
                    }
                    _ => {
                        invalid_query!();
                    }
                }
            }
        });
    }
}