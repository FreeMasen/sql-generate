use crate::{SqlWriter, Res};
use std::io::Write;
use sqlparser::ast::*;

pub struct MsSqlWriter {
    indent: usize,
    prefix: &'static str,
    current_line_len: usize,
}

impl MsSqlWriter {
    fn write_new_line(&mut self, w: &mut impl Write) -> Res<()> {
        w.write_all(b"\n")?;
        self.current_line_len = 0;
        Ok(())
    }

    fn write_prefix(&mut self, w: &mut impl Write) -> Res<()> {
        for _ in 0..self.indent {
            self.write(w, self.prefix)?;
        }
        Ok(())
    }

    fn write(&mut self, w: &mut impl Write, s: &str) -> Res<()> {
        self.current_line_len += s.chars().count();
        w.write_all(s.as_bytes())?;
        Ok(())
    }

    fn write_seperated(&mut self, w: &mut impl Write, sep: &str, idents: &[String]) -> Res<()> {
        for ref id in idents {
            self.write(w, id)?;
            self.write(w, sep)?;
        }
        Ok(())
    }
}

impl SqlWriter for MsSqlWriter {
    fn write_assignment(&mut self, node: &Assignment, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_column_def(&mut self, node: &ColumnDef, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_column_options_def(&mut self, node: &ColumnOptionDef, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_cte(&mut self, node: &Cte, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_fetch(&mut self, node: &Fetch, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_function(&mut self, node: &Function, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_join(&mut self, node: &Join, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_object_name(&mut self, node: &ObjectName, w: &mut impl Write) -> Res<()> {
        self.write_seperated(w, ".", &node.0)
    }
    fn write_order_by_expr(&mut self, node: &OrderByExpr, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_query(&mut self, node: &Query, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_select(&mut self, node: &Select, w: &mut impl Write) -> Res<()> {
        self.write(w, "SELECT ")?;
        if node.distinct {
            self.write(w, "DISTINCT ")?;
        }
        let mut after_first = false;
        for item in &node.projection {
            if after_first {
                self.write(w, ", ")?;
            }
            self.write_select_item(item, w)?;
            after_first = true;
        }
        self.write_new_line(w)?;
        self.write(w, "FROM ")?;
        for table in &node.from {
            self.write_prefix(w)?;
            self.write_table_with_joins(table, w)?;
            self.write_new_line(w)?;
        }
        self.write_new_line(w)?;
        if let Some(wh) = &node.selection {
            self.write(w, "WHERE ")?;
            self.write_expr(wh, w)?;
        }
        
        if !node.group_by.is_empty() {
            self.write(w, "GROUP BY ")?;
            let mut past_first = false;
            for group in &node.group_by {
                if past_first {
                    self.write(w, ", ")?;
                }
                self.write_expr(group, w)?;
                past_first = true;
            }
        }
        if let Some(having) = &node.having {
            self.write(w, "HAVING ")?;
            self.write_expr(having, w)?;
        }

        Ok(())
    }
    fn write_sql_option(&mut self, node: &SqlOption, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_table_alias(&mut self, node: &TableAlias, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_table_with_joins(&mut self, node: &TableWithJoins, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_values(&mut self, node: &Values, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_window_frame(&mut self, node: &WindowFrame, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_window_spec(&mut self, node: &WindowSpec, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_alter_table_operation(&mut self, node: &AlterTableOperation, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_binary_operator(&mut self, node: &BinaryOperator, w: &mut impl Write) -> Res<()> {
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
        self.write(w, s)
    }
    fn write_column_option(&mut self, node: &ColumnOption, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_data_type(&mut self, node: &DataType, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_date_time_field(&mut self, node: &DateTimeField, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_expr(&mut self, node: &Expr, w: &mut impl Write) -> Res<()> {
        match node {
            Expr::Identifier(ref id) => self.write(w, id),
            Expr::Wildcard => self.write(w, "*"),
            Expr::QualifiedWildcard(ref idents) => {
                self.write_seperated(w, ".", &idents)?;
                self.write(w, ".*")
            },
            Expr::CompoundIdentifier(ref idents) => {
                self.write_seperated(w, ".", idents)
            },
            Expr::IsNull(ref expr) => {
                self.write_expr(expr, w)?;
                self.write(w, " IS NULL")
            },
            Expr::IsNotNull(ref expr) => {
                self.write_expr(expr, w)?;
                self.write(w, " IS NOT NULL")
            },
            Expr::InList { expr, list, negated } => {
                self.write_expr(expr, w)?;
                if *negated {
                    self.write(w, " NOT")?;
                }
                self.write(w, " IN (")?;
                let mut past_first = false;
                for ref expr in list {
                    if past_first {
                        self.write(w, ", ")?;
                    }
                    self.write_expr(expr, w)?;
                    past_first = true;
                }
                self.write(w, ")")
            },
            Expr::InSubquery { expr, subquery, negated } => {
                self.write_expr(expr, w)?;
                if *negated {
                    self.write(w, " NOT")?;
                }
                self.write(w, " IN (")?;
                self.write_query(subquery, w)?;
                self.write(w, ")")
            },
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                self.write_expr(expr, w)?;
                self.write(w, " ")?;
                if *negated {
                    self.write(w, " NOT")?;
                }
                self.write(w, " BETWEEN ")?;
                self.write_expr(high, w)?;
                self.write(w, " AND ")?;
                self.write_expr(low, w)
            },
            Expr::BinaryOp { left, op, right } => {
                self.write_expr(left, w)?;
                self.write(w, " ")?;
                self.write_binary_operator(op, w)?;
                self.write(w, " ")?;
                self.write_expr(right, w)
            },
            Expr::UnaryOp { op, expr } => {
                self.write_unary_operator(op, w)?;
                self.write(w, " ")?;
                self.write_expr(expr, w)
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
                self.write_value(val, w)
            },
            Expr::Function(ref f) => {
                self.write_function(f, w)
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
                self.write_query(query, w)
            },
            Expr::Subquery(ref query) => {
                self.write_query(query, w)
            },
            _ => todo!(),
        }
    }
    fn write_file_format(&mut self, node: &FileFormat, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_join_constraint(&mut self, node: &JoinConstraint, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_join_operator(&mut self, node: &JoinOperator, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_object_type(&mut self, node: &ObjectType, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_select_item(&mut self, node: &SelectItem, w: &mut impl Write) -> Res<()> {
        match node {
            SelectItem::UnnamedExpr(ref expr) => self.write_expr(expr, w)?,
            SelectItem::ExprWithAlias { ref expr, ref alias } => {
                self.write_expr(expr, w)?;
                self.write(w, " AS ")?;
                self.write(w, &alias)?;
            },
            SelectItem::QualifiedWildcard(ref name) => {
                self.write_object_name(name, w)?;
                self.write(w, ".*")?;
            },
            SelectItem::Wildcard => self.write(w, "*")?,
        }
        Ok(())
    }
    fn write_set_expr(&mut self, node: &SetExpr, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_set_operator(&mut self, node: &SetOperator, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_set_variable_value(&mut self, node: &SetVariableValue, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_show_statement_filter(&mut self, node: &ShowStatementFilter, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_statement(&mut self, node: &Statement, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_table_constraint(&mut self, node: &TableConstraint, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_table_factor(&mut self, node: &TableFactor, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_transaction_access_mode(&mut self, node: &TransactionAccessMode, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_transaction_isolation_level(&mut self, node: &TransactionIsolationLevel, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_transaction_mode(&mut self, node: &TransactionMode, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_unary_operator(&mut self, node: &UnaryOperator, w: &mut impl Write) -> Res<()> {
        let s = match node {
            UnaryOperator::Minus => "-",
            UnaryOperator::Not => "NOT",
            UnaryOperator::Plus => "+"
        };
        self.write(w, s)
    }
    fn write_value(&mut self, node: &Value, w: &mut impl Write) -> Res<()> {
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
        self.write(w, &s)
    }
    fn write_window_frame_bound(&mut self, node: &WindowFrameBound, w: &mut impl Write) -> Res<()> {
        todo!()
    }
    fn write_window_frame_units(&mut self, node: &WindowFrameUnits, w: &mut impl Write) -> Res<()> {
        todo!()
    }
}