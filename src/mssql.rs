use crate::{SqlWriter, Res};
use std::io::Write;
use sqlparser::ast::*;

pub struct MsSqlWriter<W> {
    indent: usize,
    prefix: &'static str,
    current_line_len: usize,
    writer: W
}

impl<W> MsSqlWriter<W>
where W: Write {
    pub fn new(prefix: &'static str, writer: W) -> Self {
        Self {
            indent: 0,
            prefix,
            current_line_len: 0,
            writer
        }
    }
    fn write_new_line(&mut self) -> Res<()> {
        self.writer.write_all(b"\n")?;
        self.current_line_len = 0;
        Ok(())
    }

    fn write_prefix(&mut self) -> Res<()> {
        for _ in 0..self.indent {
            self.write(self.prefix)?;
        }
        Ok(())
    }

    fn write(&mut self, s: &str) -> Res<()> {
        self.current_line_len += s.chars().count();
        self.writer.write_all(s.as_bytes())?;
        Ok(())
    }

    fn write_separated(&mut self, sep: &str, idents: &[String]) -> Res<()> {
        let mut after_first = false;
        for ref id in idents {
            if after_first {
                self.write(sep)?;
            }
            self.write(id)?;
            after_first = true;
        }
        Ok(())
    }

    fn write_separated_expr(&mut self, sep: &str, exprs: &[Expr]) -> Res<()> {
        let mut after_first = false;
        for ref id in exprs {
            if after_first {
                self.write(sep)?;
            }
            self.write_expr(id)?;
            after_first = true;
        }
        Ok(())
    }

    pub fn into_inner(self) -> W {
        self.writer
    }
}

impl<W> SqlWriter for MsSqlWriter<W>
where W: Write {
    fn write_assignment(&mut self, node: &Assignment) -> Res<()> {
        self.write(&node.id)?;
        self.write(" = ")?;
        self.write_expr(&node.value)?;
        self.write(";\n")
    }
    fn write_column_def(&mut self, node: &ColumnDef) -> Res<()> {
        self.write(&node.name)?;
        self.write(" ")?;
        self.write_data_type(&node.data_type)?;
        if let Some(a) = &node.collation {
            self.write(" COLLATE ")?;
            self.write_object_name(a)?;
        }
        if !node.options.is_empty() {
            for opt in &node.options {
                self.write(" ")?;
                self.write_column_options_def(&opt)?;
            }
        }
        Ok(())
    }
    fn write_column_options_def(&mut self, node: &ColumnOptionDef) -> Res<()> {
        if let Some(name) = &node.name {
            self.write("CONSTRAINT ")?;
            self.write(name)?;
        }
        self.write_column_option(&node.option)
    }
    fn write_cte(&mut self, _node: &Cte) -> Res<()> {
        todo!()
    }
    fn write_fetch(&mut self, _node: &Fetch) -> Res<()> {
        todo!()
    }
    fn write_function(&mut self, _node: &Function) -> Res<()> {
        todo!()
    }
    fn write_join(&mut self, node: &Join) -> Res<()> {
        let constraint = match &node.join_operator {
            JoinOperator::Inner(con) => {
                self.write("INNER JOIN ")?;
                Some(con)
            }
            JoinOperator::LeftOuter(con) => {
                self.write("LEFT OUTER JOIN ")?;
                Some(con)
            }
            JoinOperator::FullOuter(con) => {
                self.write("FULL OUTER JOIN ")?;
                Some(con)
            }
            JoinOperator::RightOuter(con) => {
                self.write("RIGHT OUTER JOIN ")?;
                Some(con)
            }
            _ => todo!(),
        };
        self.write_table_factor(&node.relation)?;
        self.write_new_line()?;
        self.indent += 1;
        self.write_prefix()?;
        if let Some(constraint) = constraint {
            self.write_join_constraint(constraint)?;
        }
        self.indent -= 1;
        todo!()
    }
    fn write_object_name(&mut self, node: &ObjectName) -> Res<()> {
        self.write_separated(".", &node.0)
    }
    fn write_order_by_expr(&mut self, _node: &OrderByExpr) -> Res<()> {
        todo!()
    }
    fn write_query(&mut self, node: &Query) -> Res<()> {
        if !node.ctes.is_empty() {
            todo!("CTEs are not yet implemented")
        }
        self.write_set_expr(&node.body)?;
        Ok(())
    }
    fn write_select(&mut self, node: &Select) -> Res<()> {
        self.write("SELECT ")?;
        if node.distinct {
            self.write("DISTINCT ")?;
        }
        let mut after_first = false;
        for item in &node.projection {
            if after_first {
                self.write(", ")?;
            }
            self.write_select_item(item)?;
            after_first = true;
        }
        self.write_new_line()?;
        self.write("FROM ")?;
        for table in &node.from {
            self.write_prefix()?;
            self.write_table_with_joins(table)?;
        }
        if let Some(wh) = &node.selection {
            self.write_new_line()?;
            self.write("WHERE ")?;
            self.write_expr(wh)?;
        }
        
        if !node.group_by.is_empty() {
            self.write_new_line()?;
            self.write_prefix()?;
            self.write("GROUP BY ")?;
            let mut past_first = false;
            for group in &node.group_by {
                if past_first {
                    self.write(", ")?;
                }
                self.write_expr(group)?;
                past_first = true;
            }
        }
        if let Some(having) = &node.having {
            self.write_new_line()?;
            self.write_prefix()?;
            self.write("HAVING ")?;
            self.write_expr(having)?;
        }
        Ok(())
    }
    fn write_sql_option(&mut self, _node: &SqlOption) -> Res<()> {
        todo!()
    }
    fn write_table_alias(&mut self, _node: &TableAlias) -> Res<()> {
        todo!()
    }
    fn write_table_with_joins(&mut self, node: &TableWithJoins) -> Res<()> {
        match &node.relation {
            TableFactor::Table {
                ref alias,
                ref args,
                ref name,
                ref with_hints,
            } => {
                self.write_object_name(name)?;
                if let Some(ref a) = alias {
                    self.write(" AS ")?;
                    self.write_table_alias(a)?;
                }
                if !args.is_empty() {
                    self.write(" (")?;
                    self.write_separated_expr(", ", args)?;
                    self.write(")")?;
                }
                if !with_hints.is_empty() {
                    self.write(" WITH (")?;
                    self.write_separated_expr(", ", with_hints)?;
                    self.write(")")?;
                }
            },
            _ => todo!(),
        }
        Ok(())
    }
    fn write_values(&mut self, _node: &Values) -> Res<()> {
        todo!()
    }
    fn write_window_frame(&mut self, _node: &WindowFrame) -> Res<()> {
        todo!()
    }
    fn write_window_spec(&mut self, _node: &WindowSpec) -> Res<()> {
        todo!()
    }
    fn write_alter_table_operation(&mut self, _node: &AlterTableOperation) -> Res<()> {
        todo!()
    }
    fn write_binary_operator(&mut self, node: &BinaryOperator) -> Res<()> {
        let s = match node {
            BinaryOperator::And => "AND",
            BinaryOperator::Divide => "/",
            BinaryOperator::Eq => "=",
            BinaryOperator::Gt => ">",
            BinaryOperator::GtEq => ">=",
            BinaryOperator::Like => "LIKE",
            BinaryOperator::Lt => "<",
            BinaryOperator::LtEq => "<=",
            BinaryOperator::Minus => "-",
            BinaryOperator::Modulus => "%",
            BinaryOperator::Multiply => "*",
            BinaryOperator::NotEq => "!=",
            BinaryOperator::NotLike => "NOT LIKE",
            BinaryOperator::Or => "OR",
            BinaryOperator::Plus => "-",
        };
        self.write(s)
    }
    fn write_column_option(&mut self, node: &ColumnOption) -> Res<()> {
        match &node {
            ColumnOption::Null => self.write("NULL"),
            ColumnOption::NotNull => self.write("NOT NULL"),
            ColumnOption::Default(expr) => {
                self.write("DEFAULT")?;
                self.write_expr(expr)
            },
            ColumnOption::Unique { is_primary } => {
                if *is_primary {
                    self.write("PRIMARY KEY")
                } else {
                    self.write("UNIQUE")
                }
            },
            ColumnOption::ForeignKey {
                foreign_table,
                referred_columns,
            } => {
                self.write("FOREIGN KEY ")?;
                self.write_object_name(foreign_table)?;
                self.write(" (")?;
                self.write_separated(" ", referred_columns)?;
                self.write(")")
            }
            ColumnOption::Check(expr) => {
                self.write("CHECK (")?;
                self.write_expr(expr)?;
                self.write(")")
            }
        }
    }
    fn write_data_type(&mut self, _node: &DataType) -> Res<()> {
        todo!()
    }
    fn write_date_time_field(&mut self, _node: &DateTimeField) -> Res<()> {
        todo!()
    }
    fn write_expr(&mut self, node: &Expr) -> Res<()> {
        match node {
            Expr::Identifier(ref id) => self.write(id),
            Expr::Wildcard => self.write("*"),
            Expr::QualifiedWildcard(ref idents) => {
                self.write_separated(".", &idents)?;
                self.write(".*")
            },
            Expr::CompoundIdentifier(ref idents) => {
                self.write_separated(".", idents)
            },
            Expr::IsNull(ref expr) => {
                self.write_expr(expr)?;
                self.write(" IS NULL")
            },
            Expr::IsNotNull(ref expr) => {
                self.write_expr(expr)?;
                self.write(" IS NOT NULL")
            },
            Expr::InList { expr, list, negated } => {
                self.write_expr(expr)?;
                if *negated {
                    self.write(" NOT")?;
                }
                self.write(" IN (")?;
                let mut past_first = false;
                for ref expr in list {
                    if past_first {
                        self.write(", ")?;
                    }
                    self.write_expr(expr)?;
                    past_first = true;
                }
                self.write(")")
            },
            Expr::InSubquery { expr, subquery, negated } => {
                self.write_expr(expr)?;
                if *negated {
                    self.write(" NOT")?;
                }
                self.write(" IN (")?;
                self.write_query(subquery)?;
                self.write(")")
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.write_expr(expr)?;
                self.write(" ")?;
                if *negated {
                    self.write(" NOT")?;
                }
                self.write(" BETWEEN ")?;
                self.write_expr(high)?;
                self.write(" AND ")?;
                self.write_expr(low)
            },
            Expr::BinaryOp { left, op, right } => {
                self.write_expr(left)?;
                self.write(" ")?;
                self.write_binary_operator(op)?;
                self.write(" ")?;
                self.write_expr(right)
            },
            Expr::UnaryOp { op, expr } => {
                self.write_unary_operator(op)?;
                self.write(" ")?;
                self.write_expr(expr)
            },
            Expr::Cast { .. } => {
                todo!()
            },
            Expr::Extract { .. } => {
                todo!()
            },
            Expr::Collate { .. } => {
                todo!()
            },
            Expr::Nested(_expr) => {
                todo!()
            },
            Expr::Value(ref val) => {
                self.write_value(val)
            },
            Expr::Function(ref f) => {
                self.write_function(f)
            },
            Expr::Case { ..
            } => {
                todo!()
            },
            Expr::Exists(ref query) => {
                self.write_query(query)
            },
            Expr::Subquery(ref query) => {
                self.write_query(query)
            },
        }
    }
    fn write_file_format(&mut self, _node: &FileFormat) -> Res<()> {
        todo!()
    }
    fn write_join_constraint(&mut self, _node: &JoinConstraint) -> Res<()> {
        todo!()
    }
    fn write_join_operator(&mut self, _node: &JoinOperator) -> Res<()> {
        
        todo!()
    }
    fn write_object_type(&mut self, _node: &ObjectType) -> Res<()> {
        todo!()
    }
    fn write_select_item(&mut self, node: &SelectItem) -> Res<()> {
        match node {
            SelectItem::UnnamedExpr(ref expr) => self.write_expr(expr)?,
            SelectItem::ExprWithAlias { ref expr, ref alias } => {
                self.write_expr(expr)?;
                self.write(" AS ")?;
                self.write(&alias)?;
            },
            SelectItem::QualifiedWildcard(ref name) => {
                self.write_object_name(name)?;
                self.write(".*")?;
            },
            SelectItem::Wildcard => self.write("*")?,
        }
        Ok(())
    }
    fn write_set_expr(&mut self, node: &SetExpr) -> Res<()> {
        match node {
            SetExpr::Select(s) => self.write_select(s),
            SetExpr::Query(q) => self.write_query(q),
            SetExpr::SetOperation{
                ..
            } => todo!("SetExpr::SetOperation is not yet implemented"),
            SetExpr::Values(values) => self.write_values(values),
        }
    }
    fn write_set_operator(&mut self, _node: &SetOperator) -> Res<()> {
        todo!()
    }
    fn write_set_variable_value(&mut self, _node: &SetVariableValue) -> Res<()> {
        todo!()
    }
    fn write_show_statement_filter(&mut self, _node: &ShowStatementFilter) -> Res<()> {
        todo!()
    }
    fn write_statement(&mut self, node: &Statement) -> Res<()> {
        match node {
            Statement::Query(q) => self.write_query(q),
            _ => todo!()
        }
    }
    fn write_table_constraint(&mut self, _node: &TableConstraint) -> Res<()> {
        todo!()
    }
    fn write_table_factor(&mut self, _node: &TableFactor) -> Res<()> {
        todo!()
    }
    fn write_transaction_access_mode(&mut self, _node: &TransactionAccessMode) -> Res<()> {
        todo!()
    }
    fn write_transaction_isolation_level(&mut self, _node: &TransactionIsolationLevel) -> Res<()> {
        todo!()
    }
    fn write_transaction_mode(&mut self, _node: &TransactionMode) -> Res<()> {
        todo!()
    }
    fn write_unary_operator(&mut self, node: &UnaryOperator) -> Res<()> {
        let s = match node {
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
            UnaryOperator::Plus => "+"
        };
        self.write(s)
    }
    fn write_value(&mut self, node: &Value) -> Res<()> {
        let s = match node {
            Value::Boolean(b) => b.to_string(),
            Value::Date(ref s) => format!("'{}'", s),
            Value::HexStringLiteral(ref s) => format!("X'{}'", s),
            Value::NationalStringLiteral(ref s) => format!("n'{}'", s),
            Value::Number(ref s) => s.to_string(),
            Value::SingleQuotedString(ref s) 
            | Value::Time(ref s)
            | Value::Timestamp(ref s) => format!("'{}'", s),
            Value::Null => "NULL".to_string(),
            Value::Interval {
                ref value,
                ref leading_field,
                ref leading_precision,
                ref last_field,
                ref fractional_seconds_precision,
            } => {
                let mut interval = format!("INTERVAL '{}' {}", value, leading_field);
                if let Some(ref lp) = leading_precision {
                    interval.push_str(&format!("({}) ", lp))
                }
                if let Some(ref lf) = last_field {
                    interval.push_str(&format!("TO {}", lf));
                    if let Some(frac) = fractional_seconds_precision {
                        interval.push_str(&format!("({})", frac))
                    }
                }

                interval
            },
        };
        self.write(&s)
    }
    fn write_window_frame_bound(&mut self, _node: &WindowFrameBound) -> Res<()> {
        todo!()
    }
    fn write_window_frame_units(&mut self, _node: &WindowFrameUnits) -> Res<()> {
        todo!()
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn basic_select() {
        let s = Select {
            distinct: false,
            projection: vec![
               SelectItem::UnnamedExpr(Expr::Identifier("first".to_string())),
               SelectItem::UnnamedExpr(Expr::Identifier("second".to_string())),
               SelectItem::UnnamedExpr(Expr::Identifier("thrid".to_string())),
               SelectItem::UnnamedExpr(Expr::Identifier("fourth".to_string())),
               SelectItem::UnnamedExpr(Expr::Identifier("fifth".to_string())),
           ],
           from: vec![TableWithJoins {
               relation: TableFactor::Table {
                   name: ObjectName(vec!["table".to_string()]),
                   alias: None,
                   args: vec![],
                   with_hints: vec![],
               },
               joins: vec![],
           }],
           selection: None,
           group_by: vec![],
           having: None,
        };
        let mut w = MsSqlWriter {
            indent: 0,
            prefix: "    ",
            current_line_len: 0,
            writer: Vec::new(),
        };
        w.write_select(&s).unwrap();
        let v = w.into_inner();
        let out = String::from_utf8(v).unwrap();
        assert_eq!(out, "SELECT first, second, thrid, fourth, fifth
FROM table")
    }

}