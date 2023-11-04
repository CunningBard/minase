use rmp_serde::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use minase::db_core::database::Table;
use minase::db_core::query::Query;
use minase::db_core::query_error::QueryError;

pub struct Minase {
    socket: TcpStream
}

impl Minase {
    pub async fn connect(addr: &str) -> Result<Self, std::io::Error> {
        let socket = TcpStream::connect(addr).await?;
        Ok(Self {
            socket
        })
    }

    pub async fn query(&mut self, query: Query) -> Result<(), std::io::Error> {
        let mut buffer = vec![];
        query.serialize(
            &mut Serializer::new(
                &mut buffer
            )
        ).unwrap();


        println!("{}", buffer.len());

        let size = (buffer.len() as u32).to_be_bytes();
        self.socket.write_all(&size[..]).await?;
        self.socket.write_all(&buffer[..]).await?;
        println!("connection made");
        Ok(())
    }

    pub async fn receive_table(&mut self) -> Result<Table, QueryError> {
        let mut size_buffer = [0; 4];
        self.socket.read_exact(&mut size_buffer).await.unwrap();
        let size = u32::from_be_bytes(size_buffer);
        let mut buffer = vec![0; size as usize];
        self.socket.read_exact(&mut buffer).await.unwrap();
        Result::deserialize(&mut Deserializer::new(&buffer[..])).unwrap()
    }
}