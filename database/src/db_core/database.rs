use logger::Logger;
use crate::db_core::query_error::QueryError;
use crate::db_core::values::{Column, evaluate, Expr, ExprEvaluator, ToTypes, Types, Value};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    columns: Vec<Column>,
    column_types: Vec<Types>,
}

#[derive(Debug)]
pub struct Database<'a > {
    tables: Vec<Table>,
    logger: &'a mut Logger,
}

impl<'a > Database<'a > {
    pub fn new(logger: &'a mut Logger) -> Self {
        Database {
            tables: Vec::new(),
            logger
        }
    }

    pub async fn get_table(&mut self, id: usize) -> Result<&mut Table, QueryError> {
        match self.tables.get_mut(id) {
            Some(table) => {
                Ok(table)
            }
            None => {
                self.logger.error(
                    "Table Not Found".to_string(),
                    format!("table {} not found", id)
                ).await;

                Err(QueryError::TableNotFound)
            }
        }
    }

    pub async fn add_table(&mut self, columns: Vec<Column>) {
        let mut column_types = columns.iter().map(|col| col.to_types()).collect::<Vec<Types>>();

        self.tables.push(Table {
            columns,
            column_types,
        });

        self.logger.info(
            "Table Added".to_string(),
            format!("added table {} to database", self.tables.len() - 1)
        ).await;
    }

    pub async fn drop_table(&mut self, id: usize) {
        self.tables.remove(id);

        self.logger.info(
            "Table Dropped".to_string(),
            format!("dropped table {} from database", id)
        ).await;
    }

    pub async fn select(
        &mut self,
        table: usize,
        column_target: usize,

        #[allow(unused_variables)]
        condition: Vec<Expr>

    ) -> Result<Table, QueryError> {
        let mut row_ids: Vec<usize> = Vec::new();

        let target_table = match self.get_table(table).await {
            Ok(table_obj) => {
                if table_obj.columns.len() < column_target {
                    self.logger.error(
                        "Column Not Found".to_string(),
                        format!("column {} not found in table {}", column_target, table)
                    ).await;

                    return Err(QueryError::ColumnNotFound);
                }

                table_obj
            }
            Err(err) => {
                return Err(err);
            }
        };

        macro_rules! check {
            ($values:expr, $type_col:ident) => {
                for (row_id, row_value) in $values.iter().enumerate() {
                    let res = evaluate!(condition.clone(), row_value.clone(), $type_col, self.logger, table, column_target, select);
                    if let Value::Bool(true) = res {
                        row_ids.push(row_id);
                    }
                }
            };
        }

        match target_table.columns.get_mut(column_target) {
            Some(column) => {
                match column {
                    Column::Int(values) => {
                        check!(values, Int);
                    }
                    Column::Float(values) => {
                        check!(values, Float);
                    }
                    Column::String(values) => {
                        check!(values, String);
                    }
                    Column::Bool(values) => {
                        check!(values, Bool);
                    }
                }
            }
            None => {}
        }

        let mut new_columns = Vec::new();


        for column in target_table.columns.iter() {
            match column {
                Column::Int(int_col) => {
                    let mut values = vec![];
                    for row in &row_ids {
                        let value = int_col.get(*row).unwrap();
                        values.push(value.clone())
                    }
                    new_columns.push(Column::Int(values))
                }
                Column::Float(float_col) => {
                    let mut values = vec![];
                    for row in &row_ids {
                        let value = float_col.get(*row).unwrap();
                        values.push(value.clone())
                    }
                    new_columns.push(Column::Float(values))
                }
                Column::String(string_col) => {
                    let mut values = vec![];
                    for row in &row_ids {
                        let value = string_col.get(*row).unwrap();
                        values.push(value.clone())
                    }
                    new_columns.push(Column::String(values))
                }
                Column::Bool(bool_col) => {
                    let mut values = vec![];
                    for row in &row_ids {
                        let value = bool_col.get(*row).unwrap();
                        values.push(value.clone())
                    }
                    new_columns.push(Column::Bool(values))
                }
            }
        }

        let new_table = Table {
            columns: new_columns,
            column_types: target_table.column_types.clone()
        };

        self.logger.info(
            "Select".to_string(),
            format!(
                "Select query executed on table {} with column {:?} and {} rows returned",
                table,
                column_target,
                row_ids.len()
            )
        ).await;

        Ok(new_table)
    }

    pub async fn insert(&mut self, table: usize, value: Vec<Value>) -> Result<(), QueryError> {
        let target_table = self.get_table(table).await?;

        // check if the number of values is equal to the number of columns
        if value.len() != target_table.columns.len() {
            self.logger.error(
                "Size Mismatch".to_string(),
                format!("insertion on table {}: given value size is not the same as column size", table)
            ).await;

            return Err(QueryError::SizeMismatch);
        }

        // check if the values to be inserted has the correct types
        let value_types = value.iter().map(|val| val.to_types()).collect::<Vec<Types>>();

        if value_types != target_table.column_types {
            self.logger.error(
                "Type Mismatch".to_string(),
                format!("insertion on table {}: given value types are not the same as column types", table)
            ).await;

            return Err(QueryError::TypeMismatch);
        }

        macro_rules! check {
            ($id:expr, $values:expr, $val:expr, $col:ident) => {
                if let Value::$col(val) = $val {
                    $values.push(val.clone());
                } else {
                    self.logger.error(
                        "UNREACHABLE".to_string(),
                        format!("Unreachable code reached during insertion on table {}", table)
                    ).await;

                    return Err(QueryError::TypeMismatch);
                }
            };

        }

        for (id, val) in value.iter().enumerate() {
            match target_table.columns.get_mut(id) {
                Some(column) => {
                    match column {
                        Column::Int(int_vals) => {
                            check!(id, int_vals, val, Int)
                        }
                        Column::Float(float_vals) => {
                            check!(id, float_vals, val, Float)
                        }
                        Column::String(string_vals) => {
                            check!(id, string_vals, val, String)
                        }
                        Column::Bool(bool_vals) => {
                            check!(id, bool_vals, val, Bool)
                        }
                    }
                }
                None => {}
            }
        }

        self.logger.info(
            "Insert".to_string(),
            format!(
                "Insert query executed on table {} ",
                table
            )
        ).await;
        Ok(())
    }

    pub async fn update(
        &mut self,
        table: usize,
        condition_column: usize,
        target_column: usize,
        value: Value,

        #[allow(unused_variables)]
        condition: Vec<Expr>
    ) -> Result<(), QueryError> {
        let target_table = match self.tables.get_mut(table){
            Some(table_obj) => {
                if table_obj.columns.len() < condition_column || table_obj.columns.len() < target_column {
                    self.logger.error(
                        "Column Not Found".to_string(),
                        format!("column {} not found in table {}", condition_column, table)
                    ).await;

                    return Err(QueryError::ColumnNotFound);
                }
                if table_obj.column_types[target_column] != value.to_types() {
                    self.logger.error(
                        "Type Mismatch".to_string(),
                        format!("update on table {}: given value type is not the same as column type", table)
                    ).await;

                    return Err(QueryError::TypeMismatch);
                }

                table_obj
            }
            None => {
                self.logger.error(
                    "Table Not Found".to_string(),
                    format!("table {} not found", table)
                ).await;

                return Err(QueryError::TableNotFound);
            }
        };

        #[allow(unused_mut)]
        let mut row_ids: Vec<usize> = Vec::new();

        macro_rules! check {
            ($values:expr, $type_col:ident) => {
                for (row_id, row_value) in $values.iter().enumerate() {
                    let res = evaluate!(condition.clone(), row_value.clone(), $type_col, self.logger, table, condition_column, update);
                    if let Value::Bool(true) = res {
                        row_ids.push(row_id);
                    }
                }
            };
        }

        match target_table.columns.get_mut(condition_column) {
            Some(column) => {
                match column {
                    Column::Int(values) => {
                        check!(values, Int)
                    }
                    Column::Float(values) => {
                        check!(values, Float)
                    }
                    Column::String(values) => {
                        check!(values, String)
                    }
                    Column::Bool(values) => {
                        check!(values, Bool)
                    }
                }
            }
            None => {}
        }


        match target_table.columns.get_mut(target_column).unwrap() {
            Column::Int(int_col) => {
                for row in &row_ids {
                    int_col[*row] = value.clone().into_int().unwrap();
                }
            }
            Column::Float(float_col) => {
                for row in &row_ids {
                    float_col[*row] = value.clone().into_float().unwrap();
                }
            }
            Column::String(string_col) => {
                for row in &row_ids {
                    string_col[*row] = value.clone().into_string().unwrap();
                }
            }
            Column::Bool(bool_col) => {
                for row in &row_ids {
                    bool_col[*row] = value.clone().into_bool().unwrap();
                }
            }
        }
        Ok(())
    }

    pub async fn delete(&mut self, table: usize, column: usize, condition: Vec<Expr>) -> Result<(), QueryError> {
        let mut target_table = match self.get_table(table).await {
            Ok(table_obj) => {
                if table_obj.columns.len() < column {
                    self.logger.error(
                        "Column Not Found".to_string(),
                        format!("column {} not found in table {}", column, table)
                    ).await;

                    return Err(QueryError::ColumnNotFound);
                }

                table_obj
            }
            Err(err) => {
                return Err(err);
            }
        };

        #[allow(unused_mut)]
        let mut row_ids: Vec<usize> = Vec::new();

        macro_rules! check {
            ($values:expr, $type_col:ident) => {
                for (row_id, row_value) in $values.iter().enumerate() {
                    let res = evaluate!(condition.clone(), row_value.clone(), $type_col, self.logger, table, column, delete);
                    if let Value::Bool(true) = res {
                        row_ids.push(row_id);
                    }
                }
            };
        }

        match target_table.columns.get_mut(column) {
            Some(column) => {
                match column {
                    Column::Int(values) => {
                        check!(values, Int)
                    }
                    Column::Float(values) => {
                        check!(values, Float)
                    }
                    Column::String(values) => {
                        check!(values, String)
                    }
                    Column::Bool(values) => {
                        check!(values, Bool)
                    }
                }
            }
            None => {}
        }

        row_ids.reverse();

        for column in target_table.columns.iter_mut() {
            match column {
                Column::Int(ref mut int_col) => {
                    for row in &row_ids {
                        int_col.remove(*row);
                    }
                }
                Column::Float(ref mut float_col) => {
                    for row in &row_ids {
                        float_col.remove(*row);
                    }
                }
                Column::String(ref mut string_col) => {
                    for row in &row_ids {
                        string_col.remove(*row);
                    }
                }
                Column::Bool(ref mut bool_col) => {
                    for row in &row_ids {
                        bool_col.remove(*row);
                    }
                }
            }
        }

        Ok(())
    }
}