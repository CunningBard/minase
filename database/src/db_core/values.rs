use crate::db_core::query_error::QueryError;

#[derive(Clone, Debug)]
pub enum Expr {
    Value(Value),
    Column,
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Eq(Box<Expr>, Box<Expr>),
    Gt(Box<Expr>, Box<Expr>),
    Lt(Box<Expr>, Box<Expr>),
    GtEq(Box<Expr>, Box<Expr>),
    LtEq(Box<Expr>, Box<Expr>),
    Neq(Box<Expr>, Box<Expr>),
}


macro_rules! evaluate {
    (4, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs, $rhs) {
            (Value::Int(lhs), Value::Int(rhs)) => {
                Ok(Value::Bool(lhs $op rhs))
            }
            (Value::Float(lhs), Value::Float(rhs)) => {
                Ok(Value::Bool(lhs $op rhs))
            }
            (Value::String(lhs), Value::String(rhs)) => {
                Ok(Value::Bool(lhs $op rhs))
            }
            (Value::Bool(lhs), Value::Bool(rhs)) => {
                Ok(Value::Bool(lhs $op rhs))
            }
            _ => {
                Err(QueryError::TypeMismatch)
            }
        }
    };
    (1, $lhs:expr, $rhs:expr, $op:tt) => {
        match ($lhs, $rhs) {
            (Value::Bool(lhs), Value::Bool(rhs)) => {
                Ok(Value::Bool(lhs $op rhs))
            }
            _ => {
                Err(QueryError::TypeMismatch)
            }
        }
    };
    (bool, $val:expr, $op:tt) => {
        match $val {
            Value::Bool(val) => {
                Ok(Value::Bool($op val))
            }
            _ => {
                Err(QueryError::TypeMismatch)
            }
        }
    }
}

impl Expr {
    pub fn set_column_value(self, value: Value) -> Self {
        macro_rules! convert {
            ($type_given:ident, $lhs:expr, $rhs:expr) => {
                Expr::$type_given(
                    Box::new($lhs.set_column_value(value.clone())),
                    Box::new($rhs.set_column_value(value))
                )
            };
        }

        match self {
            Expr::Column => {
                Expr::Value(value)

            }
            Expr::And(lhs, rhs) => {
                convert!(And, lhs, rhs)
            }
            Expr::Or(lhs, rhs) => {
                convert!(Or, lhs, rhs)
            }
            Expr::Not(lhs) => {
                Expr::Not(Box::new(lhs.set_column_value(value)))
            }
            Expr::Eq(lhs, rhs) => {
                convert!(Eq, lhs, rhs)
            }
            Expr::Gt(lhs, rhs) => {
                convert!(Gt, lhs, rhs)
            }

            Expr::Value(..) => {
                self
            }
            Expr::Lt(lhs, rhs) => {
                convert!(Lt, lhs, rhs)
            }
            Expr::GtEq(lhs, rhs) => {
                convert!(GtEq, lhs, rhs)
            }
            Expr::LtEq(lhs, rhs) => {
                convert!(LtEq, lhs, rhs)
            }
            Expr::Neq(lhs, rhs) => {
                convert!(Neq, lhs, rhs)
            }
        }
    }

    fn evaluate_non_top(&mut self) -> Result<Value, QueryError>{
        match self {
            Expr::Value(val) => {
                Ok(val.clone())
            }
            Expr::Column => {
                Err(QueryError::ColumnValueNotSet)
            }
            Expr::And(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(1, lhs, rhs, &&)
            }
            Expr::Or(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(1, lhs, rhs, ||)
            }
            Expr::Not(val) => {
                let val = val.evaluate_non_top()?;

                evaluate!(bool, val, !)
            }
            Expr::Eq(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(4, lhs, rhs, ==)
            }
            Expr::Gt(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(4, lhs, rhs, >)
            }
            Expr::Lt(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(4, lhs, rhs, <)
            }
            Expr::GtEq(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(4, lhs, rhs, >=)
            }
            Expr::LtEq(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;

                evaluate!(4, lhs, rhs, <=)
            }
            Expr::Neq(lhs, rhs) => {
                let lhs = lhs.evaluate_non_top()?;
                let rhs = rhs.evaluate_non_top()?;


                evaluate!(4, lhs, rhs, !=)
            }
        }
    }

    pub fn evaluate_top(&mut self) -> Result<Value, QueryError>{
        match self {
            Expr::Value(..) => {
                Err(QueryError::NoOperation)
            }
            Expr::Column => {
                Err(QueryError::NoOperation)
            }
            Expr::And(..) => {
                self.evaluate_non_top()
            }
            Expr::Or(..) => {
                self.evaluate_non_top()
            }
            Expr::Not(..) => {
                self.evaluate_non_top()
            }
            Expr::Eq(..) => {
                self.evaluate_non_top()
            }
            Expr::Gt(..) => {
                self.evaluate_non_top()
            }
            Expr::Lt(..) => {
                self.evaluate_non_top()
            }
            Expr::GtEq(..) => {
                self.evaluate_non_top()
            }
            Expr::LtEq(..) => {
                self.evaluate_non_top()
            }
            Expr::Neq(..) => {
                self.evaluate_non_top()
            }
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