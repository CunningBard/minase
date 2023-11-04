use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

#[derive(Debug)]
pub struct Logger {
    writer: BufWriter<File>,
}

impl Logger {
    pub async fn new() -> Self {
        let file = File::options()
            .create(true)
            .append(true)
            .open("minase.log")
            .await.
            unwrap();

        let writer = BufWriter::new(file);
        Logger { writer }
    }

    async fn write(&mut self, msg_type: String, title: String, description: String) {
        let message = format!("{} {}: {}\n", msg_type, title, description);
        self.writer.write_all(message.as_bytes()).await.unwrap();
    }

    pub async fn error(&mut self, title: String, description: String){
        self.write("ERROR!".to_string(), title, description).await;
    }

    pub async fn warn(&mut self, title: String, description: String){
        self.write("WARNING!".to_string(), title, description).await;
    }

    pub async fn info(&mut self, title: String, description: String){
        self.write("INFO".to_string(), title, description).await;
    }

    pub async fn debug(&mut self, title: String, description: String){
        self.write("DEBUG".to_string(), title, description).await;
    }

    pub async fn flush_buffer(&mut self) {
        self.writer.flush().await.unwrap();
    }
}