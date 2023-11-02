use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug)]
pub enum QueryError {
    TypeMismatch,
    OperatorMismatch,
    NoOperation,
    ColumnValueNotSet,
    TableNotFound,
    SizeMismatch,
}

impl Display for QueryError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QueryError::TypeMismatch => {
                write!(f, "Query Error: Type Mismatch")
            }
            QueryError::OperatorMismatch => {
                write!(f, "Query Error: Operator Mismatch")
            }
            QueryError::NoOperation => {
                write!(f, "Query Error: No Operation")
            }
            QueryError::ColumnValueNotSet => {
                write!(f, "Query Error: Column Value Not Set")
            }
            QueryError::TableNotFound => {
                write!(f, "Query Error: Table Not Found")
            }
            QueryError::SizeMismatch => {
                write!(f, "Query Error: Size Mismatch")
            }
        }
    }
}

impl Error for QueryError {

}