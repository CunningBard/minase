extern crate serde;
extern crate rmp_serde as rmp;

use rmp::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use minase::db_core::database::{Database};
use minase::db_core::query::Query;

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
        let mut logger = logger::Logger::new().await;

        let (mut socket, _) = listener.accept().await?;

        let res = tokio::spawn(async move {
            let mut size_buffer = [0; 4];
            let mut buffer;

            logger.info("Database Starting".to_string(), "Execution has started".to_string()).await;

            let mut db = Database::new(
                &mut logger
            );

            // flush stdout
            // std::io::stdout().flush();

            loop {
                // db.logger_flush().await;
                // Read data from the socket
                socket.read(&mut size_buffer).await.unwrap();

                // println!("{:?}", err);

                let size = u32::from_be_bytes(size_buffer);
                buffer = vec![0; size as usize];

                let n = socket.read(&mut buffer).await.unwrap();

                if n == 0 {
                    db.logger.error("Connection Error".to_string(), "Connection has been closed".to_string()).await;
                    db.logger_flush().await;
                    return Err(());
                }


                let query = Query::deserialize(
                    &mut Deserializer::new(
                        &buffer[..]
                    )
                ).unwrap();

                db.logger_info("Query Received".to_string(), format!("{:?}", query)).await;


                match query {
                    Query::Select { table, columns, condition } => {
                        let mut buffer = vec![];

                        let table = db.select(table, columns, condition).await;
                        table.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::SelectTable { table } => {
                        let mut buffer = vec![];

                        let table = db.select_table(table).await;
                        table.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::Insert { table, values } => {
                        let mut buffer = vec![];

                        let res = db.insert(table, values).await;

                        res.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::Update { table, condition_column, targets, condition } => {
                        let mut buffer = vec![];

                        let res = db.update(table, condition_column, targets, condition).await;

                        res.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::UpdateAll { table, targets } => {
                        let mut buffer = vec![];

                        let res = db.update_all(table, targets).await;

                        res.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::Delete { table, column, condition } => {
                        let mut buffer = vec![];

                        let res = db.delete(table, column, condition).await;

                        res.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::AddTable { columns } => {
                        db.add_table(columns).await;
                    }
                    Query::DropTable { id } => {
                        let mut buffer = vec![];

                        let res = db.drop_table(id).await;

                        res.serialize(
                            &mut Serializer::new(
                                &mut buffer
                            )
                        ).unwrap();

                        let size = (buffer.len() as u32).to_be_bytes();
                        socket.write_all(&size[..]).await.unwrap();
                        socket.write_all(&buffer[..]).await.unwrap();
                    }
                    Query::Exit => {
                        db.logger.info("Database Exiting".to_string(), "Execution has ended".to_string()).await;
                        db.logger.flush_buffer().await;
                        return Ok(());
                    }
                    Query::FlushLogs => {
                        db.logger_flush().await;
                    }
                }
            }
        });

        match res.await {
            Ok(_) => {
                break
            }
            Err(_) => {
                break
            }
        }
    };

    Ok(())
}