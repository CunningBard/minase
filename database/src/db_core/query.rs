use crate::db_core::values::{Expr, Value};

#[derive(Clone, Debug)]
pub enum Query {
    Select {
        table: usize,
        columns: usize,
        condition: Expr,
    },
    Insert {
        table: usize,
        columns: usize,
        values: Value,
    },
    Update {
        table: usize,
        columns: usize,
        values: Value,
        condition: Expr,
    },
    Delete {
        table: usize,
        columns: usize,
        condition: Expr,
    },
    AddTable,
    DropTable {
        id: usize,
    },
}