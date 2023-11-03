use serde_derive::{Deserialize, Serialize};
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

    pub async fn get_table_with_column_check(&mut self, id: usize, column: usize) -> Result<&mut Table, QueryError> {
        match self.tables.get_mut(id) {
            Some(table) => {
                if table.columns.len() < column {
                    self.logger.error(
                        "Column Not Found".to_string(),
                        format!("column {} not found in table {}", column, id)
                    ).await;

                    return Err(QueryError::ColumnNotFound);
                }

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

        let target_table = self.get_table_with_column_check(table, column_target).await?;

        if condition.len() == 0 {
            self.logger.error(
                "No Condition".to_string(),
                format!("select on table {}: no condition given", table)
            ).await;

            return Err(QueryError::NoOperation);
        }

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
        targets: Vec<(usize, Vec<Expr>)>,

        #[allow(unused_variables)]
        condition: Vec<Expr>
    ) -> Result<(), QueryError> {
        let mut target_table = self.get_table_with_column_check(table, condition_column).await?;

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

        if condition.len() == 0 {
            self.logger.error(
                "No Condition".to_string(),
                format!("update on table {}: no condition given", table)
            ).await;

            return Err(QueryError::NoOperation);
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

        for (column, value) in targets {
            match target_table.columns.get_mut(column) {
                Some(column) => {
                    match column {
                        Column::Int(int_vals) => {
                            for row in &row_ids {
                                let old_value = int_vals[*row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Int, self.logger, table, condition_column, update);
                                int_vals[*row] = new_value.clone().into_int().unwrap();
                            }
                        }
                        Column::Float(float_vals) => {
                            for row in &row_ids {
                                let old_value = float_vals[*row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Float, self.logger, table, condition_column, update);
                                float_vals[*row] = new_value.clone().into_float().unwrap();
                            }
                        }
                        Column::String(string_vals) => {
                            for row in &row_ids {
                                let old_value = string_vals[*row].clone();
                                let new_value = evaluate!(value.clone(), old_value.clone(), String, self.logger, table, condition_column, update);
                                string_vals[*row] = new_value.clone().into_string().unwrap();
                            }
                        }
                        Column::Bool(bool_vals) => {
                            for row in &row_ids {
                                let old_value = bool_vals[*row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Bool, self.logger, table, condition_column, update);
                                bool_vals[*row] = new_value.clone().into_bool().unwrap();
                            }
                        }
                    }
                }
                None => {}
            }
        }

        Ok(())
    }

    pub async fn update_all(
        &mut self,
        table: usize,
        targets: Vec<(usize, Vec<Expr>)>,
    ) -> Result<(), QueryError> {
        let mut target_table = self.get_table_with_column_check(table, 0).await?;

        for (column_id, value) in targets {
            match target_table.columns.get_mut(column_id) {
                Some(column) => {
                    match column {
                        Column::Int(int_vals) => {
                            for row in 0..int_vals.len() {
                                let old_value = int_vals[row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Int, self.logger, table, column_id, update_all);
                                int_vals[row] = new_value.clone().into_int().unwrap();
                            }
                        }
                        Column::Float(float_vals) => {
                            for row in 0..float_vals.len() {
                                let old_value = float_vals[row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Float, self.logger, table, column_id, update_all);
                                float_vals[row] = new_value.clone().into_float().unwrap();
                            }
                        }
                        Column::String(string_vals) => {
                            for row in 0..string_vals.len() {
                                let old_value = string_vals[row].clone();
                                let new_value = evaluate!(value.clone(), old_value.clone(), String, self.logger, table, column_id, update_all);
                                string_vals[row] = new_value.clone().into_string().unwrap();
                            }
                        }
                        Column::Bool(bool_vals) => {
                            for row in 0..bool_vals.len() {
                                let old_value = bool_vals[row];
                                let new_value = evaluate!(value.clone(), old_value.clone(), Bool, self.logger, table, column_id, update_all);
                                bool_vals[row] = new_value.clone().into_bool().unwrap();
                            }
                        }
                    }
                }
                None => {}
            }
        }

        Ok(())
    }


    pub async fn delete(
        &mut self,
        table: usize,
        column: usize,

        #[allow(unused_variables)]
        condition: Vec<Expr>
    ) -> Result<(), QueryError> {
        let mut target_table = self.get_table_with_column_check(table, column).await?;

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

    pub async fn logger_flush(&mut self) {
        self.logger.flush_buffer().await;
    }
}