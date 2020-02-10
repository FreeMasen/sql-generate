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

    fn write_seperated(&mut self, sep: &str, idents: &[String]) -> Res<()> {
        for ref id in idents {
            self.write(id)?;
            self.write(sep)?;
        }
        Ok(())
    }
}

impl<W> SqlWriter for MsSqlWriter<W>
where W: Write {
    fn write_assignment(&mut self, node: &Assignment) -> Res<()> {
        todo!()
    }
    fn write_column_def(&mut self, node: &ColumnDef) -> Res<()> {
        todo!()
    }
    fn write_column_options_def(&mut self, node: &ColumnOptionDef) -> Res<()> {
        todo!()
    }
    fn write_cte(&mut self, node: &Cte) -> Res<()> {
        todo!()
    }
    fn write_fetch(&mut self, node: &Fetch) -> Res<()> {
        todo!()
    }
    fn write_function(&mut self, node: &Function) -> Res<()> {
        todo!()
    }
    fn write_join(&mut self, node: &Join) -> Res<()> {
        todo!()
    }
    fn write_object_name(&mut self, node: &ObjectName) -> Res<()> {
        self.write_seperated(".", &node.0)
    }
    fn write_order_by_expr(&mut self, node: &OrderByExpr) -> Res<()> {
        todo!()
    }
    fn write_query(&mut self, node: &Query) -> Res<()> {
        todo!()
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
            self.write_new_line()?;
        }
        self.write_new_line()?;
        if let Some(wh) = &node.selection {
            self.write("WHERE ")?;
            self.write_expr(wh)?;
        }
        
        if !node.group_by.is_empty() {
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
            self.write("HAVING ")?;
            self.write_expr(having)?;
        }

        Ok(())
    }
    fn write_sql_option(&mut self, node: &SqlOption) -> Res<()> {
        todo!()
    }
    fn write_table_alias(&mut self, node: &TableAlias) -> Res<()> {
        todo!()
    }
    fn write_table_with_joins(&mut self, node: &TableWithJoins) -> Res<()> {
        todo!()
    }
    fn write_values(&mut self, node: &Values) -> Res<()> {
        todo!()
    }
    fn write_window_frame(&mut self, node: &WindowFrame) -> Res<()> {
        todo!()
    }
    fn write_window_spec(&mut self, node: &WindowSpec) -> Res<()> {
        todo!()
    }
    fn write_alter_table_operation(&mut self, node: &AlterTableOperation) -> Res<()> {
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
        todo!()
    }
    fn write_data_type(&mut self, node: &DataType) -> Res<()> {
        todo!()
    }
    fn write_date_time_field(&mut self, node: &DateTimeField) -> Res<()> {
        todo!()
    }
    fn write_expr(&mut self, node: &Expr) -> Res<()> {
        match node {
            Expr::Identifier(ref id) => self.write(id),
            Expr::Wildcard => self.write("*"),
            Expr::QualifiedWildcard(ref idents) => {
                self.write_seperated(".", &idents)?;
                self.write(".*")
            },
            Expr::CompoundIdentifier(ref idents) => {
                self.write_seperated(".", idents)
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
            Expr::Cast { expr, data_type } => {
                todo!()
            },
            Expr::Extract { field, expr } => {
                todo!()
            },
            Expr::Collate { expr, collation } => {
                todo!()
            },
            Expr::Nested(expr) => {
                todo!()
            },
            Expr::Value(ref val) => {
                self.write_value(val)
            },
            Expr::Function(ref f) => {
                self.write_function(f)
            },
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                todo!()
            },
            Expr::Exists(ref query) => {
                self.write_query(query)
            },
            Expr::Subquery(ref query) => {
                self.write_query(query)
            },
            _ => todo!(),
        }
    }
    fn write_file_format(&mut self, node: &FileFormat) -> Res<()> {
        todo!()
    }
    fn write_join_constraint(&mut self, node: &JoinConstraint) -> Res<()> {
        todo!()
    }
    fn write_join_operator(&mut self, node: &JoinOperator) -> Res<()> {
        todo!()
    }
    fn write_object_type(&mut self, node: &ObjectType) -> Res<()> {
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
        todo!()
    }
    fn write_set_operator(&mut self, node: &SetOperator) -> Res<()> {
        todo!()
    }
    fn write_set_variable_value(&mut self, node: &SetVariableValue) -> Res<()> {
        todo!()
    }
    fn write_show_statement_filter(&mut self, node: &ShowStatementFilter) -> Res<()> {
        todo!()
    }
    fn write_statement(&mut self, node: &Statement) -> Res<()> {
        todo!()
    }
    fn write_table_constraint(&mut self, node: &TableConstraint) -> Res<()> {
        todo!()
    }
    fn write_table_factor(&mut self, node: &TableFactor) -> Res<()> {
        todo!()
    }
    fn write_transaction_access_mode(&mut self, node: &TransactionAccessMode) -> Res<()> {
        todo!()
    }
    fn write_transaction_isolation_level(&mut self, node: &TransactionIsolationLevel) -> Res<()> {
        todo!()
    }
    fn write_transaction_mode(&mut self, node: &TransactionMode) -> Res<()> {
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
    fn write_window_frame_bound(&mut self, node: &WindowFrameBound) -> Res<()> {
        todo!()
    }
    fn write_window_frame_units(&mut self, node: &WindowFrameUnits) -> Res<()> {
        todo!()
    }
}