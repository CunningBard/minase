use serde_derive::{Deserialize, Serialize};
use crate::db_core::values::{Expr, Types, Value};


#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Query {
    Select {
        table: usize,
        columns: usize,
        condition: Vec<Expr>,
    },
    SelectTable {
        table: usize,
    },
    Insert {
        table: usize,
        values: Vec<Value>,
    },
    Update {
        table: usize,
        condition_column: usize,
        targets: Vec<(usize, Vec<Expr>)>,
        condition: Vec<Expr>,

    },
    UpdateAll {
        table: usize,
        targets: Vec<(usize, Vec<Expr>)>,
    },
    Delete {
        table: usize,
        column: usize,
        condition: Vec<Expr>,
    },
    AddTable {
         columns: Vec<Types>
    },
    DropTable {
        id: usize,
    },
    Exit,
    FlushLogs,
}