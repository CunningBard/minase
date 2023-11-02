use crate::db_core::query_error::QueryError;

#[derive(Clone, Debug)]
pub enum Expr {
    Value(Value),
    Cell,
    Add,
    Sub,
    Mul,
    Div,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Eq,
    Neq,
    Not
}

pub struct ExprEvaluator {}

impl ExprEvaluator {
    pub fn evaluate(query: Vec<Expr>, value: Value) -> Result<Value, QueryError>{
        let mut stack = Vec::new();

        macro_rules! operation {
            (4, $left:expr, $right:expr, $op:tt) => {
                // Value::Bool because the operation that uses this are comparison operators or equality operators
                match ($left, $right) {
                    (Value::Int(left), Value::Int(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    (Value::Float(left), Value::Float(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    (Value::String(left), Value::String(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    (Value::Bool(left), Value::Bool(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    _ => {
                        return Err(QueryError::TypeMismatch);
                    }
                }
            };

            (3, $left:expr, $right:expr, $op:tt) => {
                match ($left, $right) {
                    (Value::Int(left), Value::Int(right)) => {
                        stack.push(Value::Int(left $op right));
                    }
                    (Value::Float(left), Value::Float(right)) => {
                        stack.push(Value::Float(left $op right));
                    }
                    (Value::String(left), Value::String(right)) => {
                        stack.push(Value::String(left $op &right));
                    }
                    _ => {
                        return Err(QueryError::TypeMismatch);
                    }
                }
            };

            (2, resp, $left:expr, $right:expr, $op:tt) => {
                match ($left, $right) {
                    (Value::Int(left), Value::Int(right)) => {
                        stack.push(Value::Int(left $op right));
                    }
                    (Value::Float(left), Value::Float(right)) => {
                        stack.push(Value::Float(left $op right));
                    }
                    _ => {
                        return Err(QueryError::TypeMismatch);
                    }
                }
            };
            (2, bool, $left:expr, $right:expr, $op:tt) => {
                match ($left, $right) {
                    (Value::Int(left), Value::Int(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    (Value::Float(left), Value::Float(right)) => {
                        stack.push(Value::Bool(left $op right));
                    }
                    _ => {
                        return Err(QueryError::TypeMismatch);
                    }
                }
            };

            (1, $left:expr, $right:expr, $op:tt) => {
                match ($left, $right) {
                    (Value::Int(left), Value::Int(right)) => {
                        stack.push(Value::Int(left $op right));
                    }
                    _ => {
                        return Err(QueryError::TypeMismatch);
                    }
                }
            };
        }

        let query: Vec<Expr> = query.into_iter().map(
            |e| {
                match e {
                    Expr::Cell => {
                        Expr::Value(value.clone())
                    }
                    expr=> { expr }
                }
            }
        ).collect();

        for part in query {
            match part {
                Expr::Value(val) => {
                    stack.push(val);
                }
                Expr::Cell => {
                    return Err(QueryError::CellValueNotSet);
                }
                Expr::Add => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(3, left, right, +);
                }
                Expr::Sub => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, resp, left, right, -);
                }
                Expr::Mul => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, resp, left, right, *);
                }
                Expr::Div => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, resp, left, right, /);
                }
                Expr::Gt => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, bool, left, right, >);
                }
                Expr::Lt => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, bool, left, right, <);
                }
                Expr::GtEq => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, bool, left, right, >=);
                }
                Expr::LtEq => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(2, bool, left, right, <=);
                }
                Expr::Eq => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(4, left, right, ==);
                }
                Expr::Neq => {
                    if stack.len() < 2 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    operation!(4, left, right, !=);
                }
                Expr::Not => {
                    if stack.len() < 1 {
                        return Err(QueryError::StackUnderflow);
                    }

                    let right = stack.pop().unwrap();

                    match right {
                        Value::Bool(val) => {
                            stack.push(Value::Bool(!val));
                        }
                        _ => {
                            return Err(QueryError::TypeMismatch);
                        }
                    }
                }
            }
        }

        if stack.len() != 1 {
            Err(QueryError::NoOperation)
        } else {
            Ok(stack.pop().unwrap())
        }
    }
}
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
pub enum Types {
    Int,
    Float,
    String,
    Bool,
}

pub trait ToTypes {
    fn to_types(&self) -> Types;
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(i32),
    Float(f32),
    String(String),
    Bool(bool),
}

impl ToTypes for Value {
    fn to_types(&self) -> Types {
        match self {
            Value::Int(..) => {
                Types::Int
            }
            Value::Float(..) => {
                Types::Float
            }
            Value::String(..) => {
                Types::String
            }
            Value::Bool(..) => {
                Types::Bool
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Column {
    Int(Vec<i32>),
    Float(Vec<f32>),
    String(Vec<String>),
    Bool(Vec<bool>),
}

impl ToTypes for Column {
    fn to_types(&self) -> Types {
        match self {
            Column::Int(..) => {
                Types::Int
            }
            Column::Float(..) => {
                Types::Float
            }
            Column::String(..) => {
                Types::String
            }
            Column::Bool(..) => {
                Types::Bool
            }
        }
    }
}