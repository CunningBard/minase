use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let mut minase =
        minase_driver::Minase::connect("127.0.0.1:8080").await?;

    minase.query(
        minase::db_core::query::Query::SelectTable {
            table: 0,
        }
    ).await?;
    let _ = minase.receive_table().await;

    minase.query(
        minase::db_core::query::Query::Exit
    ).await?;

    Ok(())
}
