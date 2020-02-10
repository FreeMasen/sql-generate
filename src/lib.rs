use sqlparser::ast::*;

type Res<T> = Result<T, Box<dyn std::error::Error>>;
mod mssql;
pub trait SqlWriter {
    fn write_assignment(&mut self, node: &Assignment) -> Res<()>;
    fn write_column_def(&mut self, node: &ColumnDef) -> Res<()>;
    fn write_column_options_def(&mut self, node: &ColumnOptionDef) -> Res<()>;
    fn write_cte(&mut self, node: &Cte) -> Res<()>;
    fn write_fetch(&mut self, node: &Fetch) -> Res<()>;
    fn write_function(&mut self, node: &Function) -> Res<()>;
    fn write_join(&mut self, node: &Join) -> Res<()>;
    fn write_object_name(&mut self, node: &ObjectName) -> Res<()>;
    fn write_order_by_expr(&mut self, node: &OrderByExpr) -> Res<()>;
    fn write_query(&mut self, node: &Query) -> Res<()>;
    fn write_select(&mut self, node: &Select) -> Res<()>;	
    fn write_sql_option(&mut self, node: &SqlOption) -> Res<()>;
    fn write_table_alias(&mut self, node: &TableAlias) -> Res<()>;
    fn write_table_with_joins(&mut self, node: &TableWithJoins) -> Res<()>;
    fn write_values(&mut self, node: &Values) -> Res<()>;
    fn write_window_frame(&mut self, node: &WindowFrame) -> Res<()>;
    fn write_window_spec(&mut self, node: &WindowSpec) -> Res<()>;
    fn write_alter_table_operation(&mut self, node: &AlterTableOperation) -> Res<()>;
    fn write_binary_operator(&mut self, node: &BinaryOperator) -> Res<()>;
    fn write_column_option(&mut self, node: &ColumnOption) -> Res<()>;
    fn write_data_type(&mut self, node: &DataType) -> Res<()>;
    fn write_date_time_field(&mut self, node: &DateTimeField) -> Res<()>;
    fn write_expr(&mut self, node: &Expr) -> Res<()>;
    fn write_file_format(&mut self, node: &FileFormat) -> Res<()>;
    fn write_join_constraint(&mut self, node: &JoinConstraint) -> Res<()>;
    fn write_join_operator(&mut self, node: &JoinOperator) -> Res<()>;
    fn write_object_type(&mut self, node: &ObjectType) -> Res<()>;
    fn write_select_item(&mut self, node: &SelectItem) -> Res<()>;
    fn write_set_expr(&mut self, node: &SetExpr) -> Res<()>;
    fn write_set_operator(&mut self, node: &SetOperator) -> Res<()>;
    fn write_set_variable_value(&mut self, node: &SetVariableValue) -> Res<()>;
    fn write_show_statement_filter(&mut self, node: &ShowStatementFilter) -> Res<()>;
    fn write_statement(&mut self, node: &Statement) -> Res<()>;
    fn write_table_constraint(&mut self, node: &TableConstraint) -> Res<()>;
    fn write_table_factor(&mut self, node: &TableFactor) -> Res<()>;
    fn write_transaction_access_mode(&mut self, node: &TransactionAccessMode) -> Res<()>;
    fn write_transaction_isolation_level(&mut self, node: &TransactionIsolationLevel) -> Res<()>;
    fn write_transaction_mode(&mut self, node: &TransactionMode) -> Res<()>;
    fn write_unary_operator(&mut self, node: &UnaryOperator) -> Res<()>;
    fn write_value(&mut self, node: &Value) -> Res<()>;
    fn write_window_frame_bound(&mut self, node: &WindowFrameBound) -> Res<()>;
    fn write_window_frame_units(&mut self, node: &WindowFrameUnits) -> Res<()>;
}