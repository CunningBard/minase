extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate rmp_serde as rmp;

mod db_core;

use rmp::Deserializer;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::db_core::database::{Database, Table};
use crate::db_core::values::{Column, Expr, Value};

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

            let _ = db.insert(0, vec![Value::Int(0), Value::String("Hello".into())]).await;
            let _ = db.insert(0, vec![Value::Int(1), Value::String("Hello1".into())]).await;

            loop {
                // Read data from the socket
                socket.read(&mut size_buffer).await.unwrap();

                let size = u32::from_le_bytes(size_buffer);
                buffer = vec![0; size as usize];

                let n = socket.read(&mut buffer).await.unwrap();
                if n == 0 { return; }

                let message = String::from_utf8(buffer[..n].to_owned()).unwrap();

                if message == "ready" {
                    let mut buffer = vec![];
                    db.select(
                        0,
                        0,
                        vec![
                            Expr::Cell,
                            Expr::Value(Value::Int(0)),
                            Expr::Eq
                        ])
                        .await
                        .unwrap()
                        .serialize(&mut rmp::Serializer::new(&mut buffer))
                        .unwrap();


                    let size = buffer.len() as u32;
                    let size_buffer = size.to_le_bytes();

                    socket.write(&size_buffer).await.unwrap();
                    socket.write(&buffer).await.unwrap();
                }
            }
        });
    }
}